# Lab 3 Test Results

## 3A: Leader Election

```plain
Test (3A): initial election (reliable network)...
  ... Passed --   3.0  3    58    0
Test (3A): election after network failure (reliable network)...
  ... Passed --   4.5  3   126    0
Test (3A): multiple elections (reliable network)...
  ... Passed --   5.4  7   600    0
PASS
ok      6.5840/raft1    13.077s
go test -run 3A  0.54s user 0.44s system 7% cpu 13.289 total
```

## 3B: Log Replication

```plain
Test (3B): basic agreement (reliable network)...
  ... Passed --   1.0  3    22    0
Test (3B): RPC byte count (reliable network)...
  ... Passed --   2.4  3    50    0
Test (3B): test progressive failure of followers (reliable network)...
  ... Passed --   4.9  3   124    0
Test (3B): test failure of leaders (reliable network)...
  ... Passed --   5.0  3   184    0
Test (3B): agreement after follower reconnects (reliable network)...
  ... Passed --   6.2  3   130    0
Test (3B): no agreement if too many followers disconnect (reliable network)...
  ... Passed --   3.8  5   212    0
Test (3B): concurrent Start()s (reliable network)...
  ... Passed --   0.7  3    16    0
Test (3B): rejoin of partitioned leader (reliable network)...
  ... Passed --   4.2  3   143    0
Test (3B): leader backs up quickly over incorrect follower logs (reliable network)...
  ... Passed --  26.1  5  2204    0
Test (3B): RPC counts aren't too high (reliable network)...
  ... Passed --   2.3  3    48    0
PASS
ok      6.5840/raft1    56.725s
go test -run 3B  1.49s user 0.73s system 3% cpu 56.927 total
```

## 3C: Persistence

## 3D: Log Compaction
