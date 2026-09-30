use tempfile::NamedTempFile;
use graft_core::LogEntry;
use graft_storage::SharedWal;

#[test]
fn test_shared_wal_multi_group_and_recovery() {
    let tmp = NamedTempFile::new().unwrap();
    let path = tmp.path().to_path_buf();

    {
        let mut wal = SharedWal::open(&path).unwrap();
        wal.append(
            1,
            &[
                LogEntry::new_normal(1, 1, b"g1-e1".to_vec()),
                LogEntry::new_normal(1, 2, b"g1-e2".to_vec()),
            ],
        )
        .unwrap();

        wal.append(2, &[LogEntry::new_normal(1, 1, b"g2-e1".to_vec())])
            .unwrap();

        wal.append(1, &[LogEntry::new_normal(1, 3, b"g1-e3".to_vec())])
            .unwrap();

        wal.sync().unwrap();
    }

    {
        let wal = SharedWal::open(&path).unwrap();
        let g1_entries = wal.read_group(1);
        let g2_entries = wal.read_group(2);

        assert_eq!(g1_entries.len(), 3);
        assert_eq!(g1_entries[0].data, b"g1-e1");
        assert_eq!(g1_entries[1].data, b"g1-e2");
        assert_eq!(g1_entries[2].data, b"g1-e3");

        assert_eq!(g2_entries.len(), 1);
        assert_eq!(g2_entries[0].data, b"g2-e1");
    }
}
