use magnus::{define_module, function, method, prelude::*, Error, Ruby};

mod dataset;
mod schema;
mod conversion;

use dataset::LancelotDataset;

#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    let module = define_module("Lancelot")?;
    
    let dataset_class = module.define_class("Dataset", ruby.class_object())?;
    dataset_class.define_singleton_method("new", function!(LancelotDataset::new, 1))?;
    dataset_class.define_method("path", method!(LancelotDataset::path, 0))?;
    dataset_class.define_method("create", method!(LancelotDataset::create, 1))?;
    dataset_class.define_method("open", method!(LancelotDataset::open, 0))?;
    dataset_class.define_method("add_data", method!(LancelotDataset::add_data, 1))?;
    dataset_class.define_method("count_rows", method!(LancelotDataset::count_rows, 0))?;
    dataset_class.define_method("schema", method!(LancelotDataset::schema, 0))?;
    dataset_class.define_method("scan_all", method!(LancelotDataset::scan_all, 0))?;
    dataset_class.define_method("scan_limit", method!(LancelotDataset::scan_limit, 1))?;
    dataset_class.define_method("create_vector_index", method!(LancelotDataset::create_vector_index, 1))?;
    dataset_class.define_method("vector_search", method!(LancelotDataset::vector_search, 3))?;
    dataset_class.define_method("text_search", method!(LancelotDataset::text_search, 3))?;
    dataset_class.define_method("filter_scan", method!(LancelotDataset::filter_scan, 2))?;
    
    Ok(())
}