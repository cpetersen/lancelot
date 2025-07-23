use magnus::{define_module, function, method, prelude::*, Error, Ruby};

#[magnus::init]
fn init(ruby: &Ruby) -> Result<(), Error> {
    let module = define_module("Lancelot")?;
    module.define_singleton_method("hello", function!(hello, 0))?;
    
    let dataset_class = module.define_class("Dataset", ruby.class_object())?;
    dataset_class.define_singleton_method("new", function!(LancelotDataset::new, 1))?;
    dataset_class.define_method("path", method!(LancelotDataset::path, 0))?;
    
    Ok(())
}

fn hello() -> &'static str {
    "Hello from Lancelot with Lance!"
}

#[derive(Debug)]
#[magnus::wrap(class = "Lancelot::Dataset")]
struct LancelotDataset {
    path: String,
}

impl LancelotDataset {
    fn new(path: String) -> Self {
        Self { path }
    }
    
    fn path(&self) -> &str {
        &self.path
    }
}