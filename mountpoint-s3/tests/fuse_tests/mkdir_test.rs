use std::fs::{self, metadata, DirBuilder, File};

use test_case::test_case;

use crate::fuse_tests::{read_dir_to_entry_names, SessionCreator};

fn mkdir_test(creator_fn: SessionCreator, prefix: &str) {
    let (mount_point, _session, mut test_client) = creator_fn(prefix, Default::default());

    // Create local directory
    let dirname = "local_dir";
    let dirpath = mount_point.path().join(dirname);

    DirBuilder::new().recursive(true).create(&dirpath).unwrap();

    assert!(!test_client.contains_dir(dirname).unwrap());

    let m = metadata(&dirpath).unwrap();
    assert!(m.file_type().is_dir());

    // Write an object into the directory
    let filename = "nested_file";
    {
        let filepath = dirpath.join(filename);
        let f = File::options().write(true).create(true).open(filepath).unwrap();
        f.sync_all().unwrap();
    }

    // Remove the new object from the client
    test_client.remove_object(&format!("{dirname}/{filename}")).unwrap();

    // Verify that the directory disappeared
    let read_dir_iter = fs::read_dir(mount_point.path()).unwrap();
    let dir_entry_names = read_dir_to_entry_names(read_dir_iter);
    assert_eq!(dir_entry_names, Vec::<String>::new());
}

#[cfg(feature = "s3_tests")]
#[test]
fn mkdir_test_s3() {
    mkdir_test(crate::fuse_tests::s3_session::new, "mkdir_test");
}

#[test_case(""; "unprefixed")]
#[test_case("test_prefix/"; "prefixed")]
fn mkdir_test_mock(prefix: &str) {
    mkdir_test(crate::fuse_tests::mock_session::new, prefix);
}
