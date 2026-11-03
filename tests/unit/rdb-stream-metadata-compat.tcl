# RDB v11 stream fixtures: an empty stream with UINT64_MAX first-ID
# components, and a nonempty stream whose group has entries-read = -1.
# The latter was generated with XADD 1-0 field value and
# XGROUP CREATE group 0 ENTRIESREAD -1; both groups have no consumers/PEL.
# These metadata fields are discarded by Redis 6, but must still be read.
set stream_metadata_fixtures {
    empty 130000000081ffffffffffffffff81ffffffffffffffff000000010567726f757000000000000b006809e6e378bc6d13
    unknown 1301100000000000000001000000000000000025250000000a00010100010101856669656c640600010201000100018576616c7565060401ff0101000100000001010567726f7570000081ffffffffffffffff00000b0001c320f9e02edd3f
}

start_server {tags {"rdb" "dump" "downgrade"}} {
    foreach {name hex} $stream_metadata_fixtures {
        test "RESTORE newer stream with UINT64_MAX $name metadata" {
            r config set rdb-version-check relaxed
            assert_equal OK [r restore $name 0 [binary decode hex $hex]]
            assert_equal 1 [llength [r xinfo groups $name]]
            assert_equal 0 [lindex [r xpending $name group] 0]
            if {$name eq "empty"} {
                assert_equal 0 [r xlen $name]
                assert_equal {} [r xrange $name - +]
            } else {
                assert_equal {{1-0 {field value}}} [r xrange $name - +]
                assert_equal {{1-0 {field value}}} [r xrevrange $name + -]
            }
            r config set rdb-version-check strict
        }
    }
}

foreach {name hex} $stream_metadata_fixtures {
    # Use the same object in a full RDB followed by another key, verifying
    # that skipping newer metadata preserves the next object's alignment.
    # A zero checksum denotes a snapshot saved with checksumming disabled.
    set object [binary decode hex [string range $hex 0 end-20]]
    set prefix "REDIS0011[string index $object 0]\x06stream"
    set suffix "\x00\x05after\x02ok\xff[string repeat \x00 8]"
    set server_path [tmpdir "server.stream-metadata-$name"]
    set fd [open "$server_path/dump.rdb" w]
    fconfigure $fd -translation binary
    puts -nonewline $fd "$prefix[string range $object 1 end]$suffix"
    close $fd

    start_server [list tags {"rdb" "downgrade"} overrides [list dir $server_path dbfilename dump.rdb]] {
        test "Load full RDB with UINT64_MAX $name stream metadata" {
            r select 0
            assert_equal ok [r get after]
            assert_equal 1 [llength [r xinfo groups stream]]
            assert_equal 0 [lindex [r xpending stream group] 0]
            if {$name eq "empty"} {
                assert_equal 0 [r xlen stream]
            } else {
                assert_equal {{1-0 {field value}}} [r xrange stream - +]
            }
            r debug reload
            assert_equal ok [r get after]
            assert_equal 1 [llength [r xinfo groups stream]]
        }
    }

    test "Reject truncated $name stream metadata in a full RDB" {
        # Stop inside the first UINT64_MAX for the empty stream, or inside
        # the group's entries-read field for the nonempty stream.
        if {$name eq "empty"} {
            set truncated [string range $object 1 9]
        } else {
            set truncated [string range $object 1 end-5]
        }
        set fd [open "$server_path/truncated.rdb" w]
        fconfigure $fd -translation binary
        puts -nonewline $fd "$prefix$truncated"
        close $fd
        set status [catch {
            exec src/redis-check-rdb "$server_path/truncated.rdb" 2>@1
        } output]
        assert_equal 1 $status
        if {$name eq "empty"} {
            assert_match "*Failed to load extended stream metadata*" $output
        } else {
            assert_match "*Failed to load consumer group offset*" $output
        }
    }
}
