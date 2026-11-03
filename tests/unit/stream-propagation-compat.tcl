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
    }
}
