# Boundary payloads must remain readable after downgrade, not merely load.
source tests/assets/listpack-downgrade-fixtures.tcl

proc listpack_compat_state {kind} {
    switch -glob -- $kind {
        stream* { return [r xrange compat - +] }
        list { return [r lrange compat 0 -1] }
        hash {
            set result {}
            foreach key [lsort [r hkeys compat]] {
                lappend result $key [r hget compat $key]
            }
            return $result
        }
        set { return [lsort [r smembers compat]] }
        zset { return [r zrange compat 0 -1 withscores] }
    }
}

start_server {tags {"rdb" "dump" "downgrade" "external:skip"}} {
    test {Core listpack dual-width traversal, mutation and normalization} {
        exec make -C src test-listpack 2>@1
    }

    r config set rdb-version-check relaxed
    r config set rdbcompression no
    foreach {kind length encoded canonical} $listpack_compat_valid {
        test "Downgrade $kind with $length-byte boundary value survives reads and reload" {
            set value [string repeat x $length]
            set replacement [string repeat y $length]
            set payload [zlib decompress [binary decode hex $encoded]]
            assert_equal OK [r restore compat 0 $payload replace]
            switch -glob -- $kind {
                stream* {
                    assert_equal [list [list 1-0 [list f $value]] [list 2-0 {f tail}]] [r xrange compat - +]
                    assert_equal [list [list 2-0 {f tail}] [list 1-0 [list f $value]]] [r xrevrange compat + -]
                    # Version 9/type 15 and newer types must all serialize the
                    # same historical backlens, including for old downstreams.
                    assert_equal [zlib decompress [binary decode hex $canonical]] [r dump compat]
                }
                list {
                    assert_equal [list $value tail] [r lrange compat 0 -1]
                    assert_equal tail [r lindex compat -1]
                }
                hash {
                    assert_equal {a b f} [lsort [r hkeys compat]]
                    assert_equal $value [r hget compat f]
                    assert_equal 2 [r hget compat b]
                }
                set { assert_equal [lsort [list a $value b]] [lsort [r smembers compat]] }
                zset {
                    assert_equal [list a 1 $value 2 b 3] [r zrange compat 0 -1 withscores]
                    assert_equal [list b 3 $value 2 a 1] [r zrevrange compat 0 -1 withscores]
                }
            }
            set expected [listpack_compat_state $kind]
            r debug reload
            assert_equal $expected [listpack_compat_state $kind]

            switch -glob -- $kind {
                stream* {
                    r xadd compat 3-0 f appended
                    assert_equal 1 [r xdel compat 1-0]
                    assert_equal 1 [r xtrim compat maxlen 1]
                    assert_equal {{3-0 {f appended}}} [r xrange compat - +]
                }
                list {
                    assert_equal OK [r lset compat 0 $replacement]
                    assert_equal [list $replacement tail] [r lrange compat 0 -1]
                    assert_equal $replacement [r lpop compat]
                }
                hash {
                    r hset compat f $replacement
                    assert_equal $replacement [r hget compat f]
                    assert_equal 1 [r hdel compat b]
                }
                set {
                    assert_equal 1 [r srem compat $value]
                    assert_equal {a b} [lsort [r smembers compat]]
                }
                zset {
                    assert_equal 3 [r zincrby compat 1 $value]
                    assert_equal 1 [r zrem compat b]
                    assert_equal [list a 1 $value 3] [r zrange compat 0 -1 withscores]
                }
            }
            r set after intact
            set expected [listpack_compat_state $kind]
            r debug reload
            assert_equal intact [r get after]
            assert_equal $expected [listpack_compat_state $kind]
        }
    }

    foreach {kind damage encoded} $listpack_compat_invalid {
        test "Reject malformed $kind listpack: $damage" {
            set payload [zlib decompress [binary decode hex $encoded]]
            assert_error {ERR Bad data format} {r restore invalid 0 $payload replace}
            assert_equal 0 [r exists invalid]
            assert_equal PONG [r ping]
        }
    }
}

# Exercise raw stream listpacks through full RDB startup, not just RESTORE.
foreach {kind length encoded canonical} $listpack_compat_valid {
    if {$length != 16378 || ($kind ne "stream15" && $kind ne "stream21")} continue
    set object [string range [zlib decompress [binary decode hex $encoded]] 0 end-10]
    set header [expr {$kind eq "stream15" ? "REDIS0009" : "VALKEY080"}]
    set data "$header[string index $object 0]\x06compat[string range $object 1 end]"
    append data "\x00\x05after\x06intact\xff[string repeat \x00 8]"
    set server_path [tmpdir "server.listpack-$kind"]
    set fd [open "$server_path/dump.rdb" w]
    fconfigure $fd -translation binary
    puts -nonewline $fd $data
    close $fd
    start_server [list tags {"rdb" "downgrade" "external:skip"} overrides [list dir $server_path dbfilename dump.rdb]] {
        test "Full RDB load normalizes $kind and preserves subsequent key alignment" {
            r select 0
            r config set rdbcompression no
            assert_equal intact [r get after]
            assert_equal 2 [r xlen compat]
            assert_equal [zlib decompress [binary decode hex $canonical]] [r dump compat]
            r debug reload
            assert_equal 2 [r xlen compat]
            assert_equal intact [r get after]
        }
    }
}
