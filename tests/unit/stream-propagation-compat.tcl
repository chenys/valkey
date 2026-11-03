start_server {tags {"stream downgrade"}} {
    test {SETID accepts Valkey ENTRIESREAD while retaining Redis 6 cursor behavior} {
        r XADD stream 1-0 field one
        r XADD stream 2-0 field two
        r XGROUP CREATE stream group 0
        foreach counter {-1 0 1 9223372036854775807} {
            assert_equal OK [r XGROUP SETID stream group 1-0 ENTRIESREAD $counter]
            assert_equal 1-0 [dict get [lindex [r XINFO GROUPS stream] 0] last-delivered-id]
        }
        assert_equal OK [r XGROUP SETID stream group $ entriesread -1]
        assert_equal 2-0 [dict get [lindex [r XINFO GROUPS stream] 0] last-delivered-id]
        assert_equal OK [r XGROUP SETID stream group 0]
        assert_equal 0-0 [dict get [lindex [r XINFO GROUPS stream] 0] last-delivered-id]
    }

    test {Malformed SETID metadata does not change the cursor} {
        foreach options {
            {ENTRIESREAD -2}
            {ENTRIESREAD 9223372036854775808}
            {ENTRIESREAD invalid}
            {ENTRIESREAD 1.5}
            {UNKNOWN 1}
            {ENTRIESREAD}
            {ENTRIESREAD 1 extra}
        } {
            assert_error ERR* {r XGROUP SETID stream group 2-0 {*}$options}
            assert_equal 0-0 [dict get [lindex [r XINFO GROUPS stream] 0] last-delivered-id]
        }
        assert_error "*Invalid stream ID*" {r XGROUP SETID stream group invalid ENTRIESREAD -1}
        assert_equal 0-0 [dict get [lindex [r XINFO GROUPS stream] 0] last-delivered-id]
        assert_error NOGROUP* {r XGROUP SETID stream missing 1-0 ENTRIESREAD -1}
        assert_error ERR* {r XGROUP SETID missing group 1-0 ENTRIESREAD -1}
        r SET string value
        assert_error WRONGTYPE* {r XGROUP SETID string group 1-0 ENTRIESREAD -1}
    }

    test {CREATECONSUMER is idempotent and preserves pending entries and idle time} {
        assert_equal 1 [r XGROUP CREATECONSUMER stream group consumer]
        assert_equal 0 [r XGROUP CREATECONSUMER stream group consumer]
        assert_equal 0 [dict get [lindex [r XINFO CONSUMERS stream group] 0] pending]
        r XREADGROUP GROUP group consumer COUNT 1 STREAMS stream >
        set pending [r XPENDING stream group - + 10]
        r DEBUG SLEEP 0.05
        set idle [dict get [lindex [r XINFO CONSUMERS stream group] 0] idle]
        set dirty [s rdb_changes_since_last_save]
        assert_equal 0 [r XGROUP CREATECONSUMER stream group consumer]
        assert_equal $dirty [s rdb_changes_since_last_save]
        assert {[dict get [lindex [r XINFO CONSUMERS stream group] 0] idle] >= $idle}
        set after [r XPENDING stream group - + 10]
        assert_equal [lindex $pending 0 0] [lindex $after 0 0]
        assert_equal [lindex $pending 0 1] [lindex $after 0 1]
        assert_equal [lindex $pending 0 3] [lindex $after 0 3]
        assert_equal 1 [r XGROUP CREATECONSUMER stream group empty]
    }

    test {CREATECONSUMER validates key, group and argument count} {
        assert_error ERR* {r XGROUP CREATECONSUMER missing group consumer}
        assert_error WRONGTYPE* {r XGROUP CREATECONSUMER string group consumer}
        assert_error NOGROUP* {r XGROUP CREATECONSUMER stream missing consumer}
        assert_error ERR* {r XGROUP CREATECONSUMER stream group}
        assert_error ERR* {r XGROUP CREATECONSUMER stream group consumer extra}
        assert_equal 2 [llength [r XINFO CONSUMERS stream group]]
    }

    test {CREATECONSUMER empty and nonempty consumers survive RDB reload} {
        r DEBUG RELOAD
        assert_equal {consumer empty} [lmap item [r XINFO CONSUMERS stream group] {dict get $item name}]
        assert_equal {1 0} [lmap item [r XINFO CONSUMERS stream group] {dict get $item pending}]
    }

    start_server {} {
        set primary [srv -1 client]
        set replica [srv 0 client]
        test {Valkey SETID metadata replicates without errors or cursor loss} {
            $replica REPLICAOF [srv -1 host] [srv -1 port]
            wait_for_condition 50 100 {
                [s 0 master_link_status] eq {up}
            } else {
                fail "Replication did not start"
            }
            $replica CONFIG RESETSTAT
            $primary XGROUP SETID stream group 2-0 ENTRIESREAD -1
            assert_equal 1 [$primary WAIT 1 5000]
            assert_equal 2-0 [dict get [lindex [$replica XINFO GROUPS stream] 0] last-delivered-id]
            assert_equal 0 [s 0 unexpected_error_replies]
            $replica REPLICAOF NO ONE
            assert_equal {} [$replica XREADGROUP GROUP group consumer NOACK STREAMS stream >]
        }

        test {CREATECONSUMER replicates new empty consumers and is safe to replay} {
            $replica REPLICAOF [srv -1 host] [srv -1 port]
            wait_for_condition 50 100 {
                [s 0 master_link_status] eq {up}
            } else {
                fail "Replication did not restart"
            }
            assert_equal 1 [$primary XGROUP CREATECONSUMER stream group replicated]
            assert_equal 1 [$primary WAIT 1 5000]
            assert_equal 0 [$primary XGROUP CREATECONSUMER stream group replicated]
            assert_equal {consumer empty replicated} [lmap item [$replica XINFO CONSUMERS stream group] {dict get $item name}]
            assert_equal 0 [s 0 unexpected_error_replies]
        }
    }
}

start_server {tags {"stream downgrade aof"} overrides {appendonly yes aof-use-rdb-preamble no}} {
    test {CREATECONSUMER preserves empty consumers across AOF replay and rewrite} {
        r XADD stream 1-0 field value
        r XGROUP CREATE stream group 0
        r XREADGROUP GROUP group pending STREAMS stream >
        r XGROUP CREATECONSUMER stream group empty
        r XGROUP SETID stream group 2-0 ENTRIESREAD -1
        foreach rewrite {0 1} {
            if {$rewrite} {
                r BGREWRITEAOF
                waitForBgrewriteaof r
            }
            r DEBUG LOADAOF
            assert_equal {empty pending} [lmap item [r XINFO CONSUMERS stream group] {dict get $item name}]
            assert_equal {0 1} [lmap item [r XINFO CONSUMERS stream group] {dict get $item pending}]
            assert_equal 2-0 [dict get [lindex [r XINFO GROUPS stream] 0] last-delivered-id]
            assert_equal 1-0 [lindex [r XPENDING stream group - + 10] 0 0]
        }
    }
}
