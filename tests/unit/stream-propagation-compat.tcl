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

    foreach removal {delete trim} {
        foreach options {{} FORCE JUSTID {FORCE JUSTID}} {
            test "XCLAIM cleans $removal tombstones before idle checks ($options)" {
                r DEL claim
                r XADD claim 1-0 field one
                r XADD claim 2-0 field two
                r XGROUP CREATE claim group 0
                r XREADGROUP GROUP group old STREAMS claim >
                if {$removal eq "delete"} {
                    r XDEL claim 1-0
                } else {
                    r XTRIM claim MAXLEN 1
                }
                assert_equal {} [r XCLAIM claim group new 999999999 1-0 1-0 999-0 {*}$options]
                assert_equal {new old} [lmap item [r XINFO CONSUMERS claim group] {dict get $item name}]
                assert_equal {0 1} [lmap item [r XINFO CONSUMERS claim group] {dict get $item pending}]
                set pending [r XPENDING claim group - + 10]
                assert_equal 1 [llength $pending]
                assert_equal 2-0 [lindex $pending 0 0]
                assert_equal old [lindex $pending 0 1]
                assert_equal 1 [lindex $pending 0 3]
                assert_equal 1 [r XACK claim group 2-0]
                assert_equal 0 [r XGROUP DELCONSUMER claim group old]
            }
        }
    }

    test {XCLAIM retains FORCE, JUSTID, retry count, delivery time and idle semantics} {
        r DEL claim
        r XADD claim 1-0 field one
        r XGROUP CREATE claim group 0
        assert_equal {1-0} [r XCLAIM claim group old 0 1-0 999-0 FORCE JUSTID TIME 1 RETRYCOUNT 7]
        set pending [r XPENDING claim group - + 10]
        assert_equal 7 [lindex $pending 0 3]
        assert {[lindex $pending 0 2] > 1000}
        assert_equal {1-0} [r XCLAIM claim group new 0 1-0 JUSTID IDLE 0]
        assert_equal 7 [lindex [r XPENDING claim group - + 10] 0 3]
        assert_equal {} [r XCLAIM claim group skipped 999999999 1-0]
        assert_equal new [lindex [r XPENDING claim group - + 10] 0 1]
        assert_equal {{1-0 {field one}}} [r XCLAIM claim group old 0 1-0]
        assert_equal 8 [lindex [r XPENDING claim group - + 10] 0 3]
        assert_error ERR* {r XCLAIM claim group invalid 0 1-0 UNKNOWN}
        assert_equal {new old skipped} [lmap item [r XINFO CONSUMERS claim group] {dict get $item name}]
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

        foreach mode {missing lastid deleted trimmed idle success} {
            test "XCLAIM consumer and PEL changes replicate consistently ($mode)" {
                $primary DEL claim
                $primary XADD claim 1-0 field one
                $primary XADD claim 2-0 field two
                $primary XGROUP CREATE claim group 0
                $primary XREADGROUP GROUP group old STREAMS claim >
                switch $mode {
                    missing {$primary XCLAIM claim group new 0 999-0}
                    lastid {$primary XCLAIM claim group new 0 999-0 LASTID 1000-0}
                    deleted {
                        $primary XDEL claim 1-0
                        $primary XCLAIM claim group new 999999999 1-0
                    }
                    trimmed {
                        $primary XTRIM claim MAXLEN 1
                        $primary XCLAIM claim group new 999999999 1-0 FORCE JUSTID
                    }
                    idle {$primary XCLAIM claim group new 999999999 1-0}
                    success {$primary XCLAIM claim group new 0 1-0}
                }
                assert_equal 1 [$primary WAIT 1 5000]
                foreach node [list $primary $replica] {
                    assert_equal {new old} [lmap item [$node XINFO CONSUMERS claim group] {dict get $item name}]
                }
                assert_equal [$primary XINFO GROUPS claim] [$replica XINFO GROUPS claim]
                set expected [$primary XPENDING claim group - + 10]
                set actual [$replica XPENDING claim group - + 10]
                assert_equal [lmap item $expected {list [lindex $item 0] [lindex $item 1] [lindex $item 3]}] \
                    [lmap item $actual {list [lindex $item 0] [lindex $item 1] [lindex $item 3]}]
                assert_equal [lmap item [$primary XINFO CONSUMERS claim group] {dict get $item pending}] \
                    [lmap item [$replica XINFO CONSUMERS claim group] {dict get $item pending}]
                assert_equal 0 [s 0 unexpected_error_replies]
            }
        }

        test {Cleaned PEL and empty consumers survive promotion and RDB reload} {
            $primary XDEL claim 1-0
            $primary XCLAIM claim group empty 0 1-0
            assert_equal 1 [$primary WAIT 1 5000]
            $replica REPLICAOF NO ONE
            $replica DEBUG RELOAD
            assert_equal {empty new old} [lmap item [$replica XINFO CONSUMERS claim group] {dict get $item name}]
            assert_equal {0 0 1} [lmap item [$replica XINFO CONSUMERS claim group] {dict get $item pending}]
            assert_equal 2-0 [lindex [$replica XPENDING claim group - + 10] 0 0]
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

    test {Deleted PEL cleanup and new empty claim consumers survive AOF replay and rewrite} {
        r XDEL stream 1-0
        assert_equal {} [r XCLAIM stream group new 999999999 1-0 999-0 FORCE JUSTID LASTID 1000-0]
        foreach rewrite {0 1} {
            if {$rewrite} {
                r BGREWRITEAOF
                waitForBgrewriteaof r
            }
            r DEBUG LOADAOF
            assert_equal {empty new pending} [lmap item [r XINFO CONSUMERS stream group] {dict get $item name}]
            assert_equal {0 0 0} [lmap item [r XINFO CONSUMERS stream group] {dict get $item pending}]
            assert_equal {} [r XPENDING stream group - + 10]
            assert_equal 1000-0 [dict get [lindex [r XINFO GROUPS stream] 0] last-delivered-id]
        }
    }
}
