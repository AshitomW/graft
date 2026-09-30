use graft_blob::PayloadManager;

#[test]
fn test_small_payload_inline() {
    let mut manager = PayloadManager::new(4096);
    let small_data = b"small inline command".to_vec();

    let encoded = manager.prepare_proposal(small_data.clone());
    assert_eq!(encoded[0], 0x00);
    assert_eq!(manager.store().len(), 0);

    let resolved = manager.resolve_entry(&encoded).unwrap();
    assert_eq!(resolved, small_data);
}

#[test]
fn test_large_payload_out_of_band() {
    let mut manager = PayloadManager::new(1024);
    let large_data = vec![42u8; 8192];

    let encoded = manager.prepare_proposal(large_data.clone());
    assert_eq!(encoded[0], 0x01);
    assert_eq!(manager.store().len(), 1);
    assert!(encoded.len() < 200);

    let resolved = manager.resolve_entry(&encoded).unwrap();
    assert_eq!(resolved, large_data);
}
