use crate::storage::Pager;
use crate::storage::paging::traits::{Page, PageMut};

pub mod buffered_pager;
pub mod caching_pager;
pub mod file_pager;
pub mod slotted_pager;
pub mod traits;
pub mod virtual_pager;

#[cfg(test)]
pub mod tests {
    use test_log::test;
    use tempfile::tempdir;
    use crate::storage::{Pager, paging};
    use crate::storage::paging::buffered_pager::BufferedPager;
    use crate::storage::paging::file_pager::FilePager;
    use crate::storage::paging::traits::{Page, PageMut};
    use crate::storage::paging::virtual_pager::VirtualPagerTable;

    pub fn pager_reusable<P: Pager>(create_pager: impl Fn() -> P) {
        {
            let pager = create_pager();
            let (mut page, 0) = pager.new_page().expect("could not create a new page") else {
                panic!("expected 0 id for page")
            };
            // write a known magic number at a given offset
            page.write_u64(0xDEADBEEF, 16);
        }
        {
            let pager = create_pager();
            let page = pager.get(0).expect("could not create a new page");
            // reada known magic number at a given offset
            let magic = page.read_u64(16).expect("magic number should be present");
            assert_eq!(magic, 0xDEADBEEF);
        }
    }

    #[test]
    fn deeply_nested_pager() {
        let dir = tempdir().unwrap();
        let file_path = dir.as_ref().join("tempfile.vpt");
        pager_reusable(|| {
            let pager = FilePager::open_or_create(&file_path).unwrap();
            let buffered = BufferedPager::new(pager);
            let vp_table = VirtualPagerTable::<usize, _>::new(buffered).unwrap();
            let vp = vp_table.get_or_init(0).unwrap();
            vp
        });
    }

}

