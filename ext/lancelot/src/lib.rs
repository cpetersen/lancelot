use magnus::{
    define_module, function, method, prelude::*, 
    Error, Ruby, RHash, RArray, Symbol, Value, TryConvert, r_hash::ForEach
};
use std::cell::RefCell;
use std::sync::Arc;
use tokio::runtime::Runtime;
use lance::Dataset;
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use arrow_array::{RecordBatch, RecordBatchIterator, StringArray, Float32Array, ArrayRef, Array};
use std::collections::HashMap;
use futures::stream::{StreamExt, TryStreamExt};

#[magnus::wrap(class = "Lancelot::Dataset", free_immediately, size)]
struct LancelotDataset {
    dataset: RefCell<Option<Dataset>>,
    runtime: RefCell<Runtime>,
    path: String,
}

impl LancelotDataset {
    fn new(path: String) -> Result<Self, Error> {
        let runtime = Runtime::new()
            .map_err(|e| Error::new(magnus::exception::runtime_error(), e.to_string()))?;
        
        Ok(Self {
            dataset: RefCell::new(None),
            runtime: RefCell::new(runtime),
            path,
        })
    }

    fn path(&self) -> String {
        self.path.clone()
    }

    fn create(&self, schema_hash: RHash) -> Result<(), Error> {
        let schema = build_arrow_schema(schema_hash)?;
        
        let empty_batch = RecordBatch::new_empty(Arc::new(schema.clone()));
        let batches = vec![empty_batch];
        let reader = RecordBatchIterator::new(
            batches.into_iter().map(Ok),
            Arc::new(schema)
        );
        
        let dataset = self.runtime.borrow_mut().block_on(async {
            Dataset::write(
                reader,
                &self.path,
                None,
            )
            .await
            .map_err(|e| Error::new(magnus::exception::runtime_error(), e.to_string()))
        })?;

        self.dataset.replace(Some(dataset));
        Ok(())
    }

    fn open(&self) -> Result<(), Error> {
        let dataset = self.runtime.borrow_mut().block_on(async {
            Dataset::open(&self.path)
                .await
                .map_err(|e| Error::new(magnus::exception::runtime_error(), e.to_string()))
        })?;

        self.dataset.replace(Some(dataset));
        Ok(())
    }

    fn add_data(&self, data: RArray) -> Result<(), Error> {
        let mut dataset = self.dataset.borrow_mut();
        let dataset = dataset.as_mut()
            .ok_or_else(|| Error::new(magnus::exception::runtime_error(), "Dataset not opened"))?;

        // TODO: Use actual dataset schema once we figure out Lance 0.31 API
        // For now, we'll use a simplified schema
        let arrow_schema = ArrowSchema::new(vec![
            Field::new("text", DataType::Utf8, true),
            Field::new("score", DataType::Float32, true),
        ]);

        let batch = build_record_batch(data, &arrow_schema)?;

        let batches = vec![batch];
        let reader = RecordBatchIterator::new(
            batches.into_iter().map(Ok),
            Arc::new(arrow_schema)
        );
        
        self.runtime.borrow_mut().block_on(async move {
            dataset.append(reader, None)
                .await
                .map_err(|e| Error::new(magnus::exception::runtime_error(), e.to_string()))
        })?;

        Ok(())
    }

    fn count_rows(&self) -> Result<i64, Error> {
        let dataset = self.dataset.borrow();
        let dataset = dataset.as_ref()
            .ok_or_else(|| Error::new(magnus::exception::runtime_error(), "Dataset not opened"))?;

        let count = self.runtime.borrow_mut().block_on(async {
            dataset.count_rows(None)
                .await
                .map_err(|e| Error::new(magnus::exception::runtime_error(), e.to_string()))
        })?;

        Ok(count as i64)
    }

    fn schema(&self) -> Result<RHash, Error> {
        let dataset = self.dataset.borrow();
        let _dataset = dataset.as_ref()
            .ok_or_else(|| Error::new(magnus::exception::runtime_error(), "Dataset not opened"))?;

        let ruby = Ruby::get().unwrap();
        let hash = ruby.hash_new();
        
        // TODO: Read actual schema from Lance dataset once we figure out the 0.31 API
        // For now, return a hardcoded schema that matches what we support
        hash.aset(Symbol::new("text"), "string")?;
        hash.aset(Symbol::new("score"), "float32")?;

        Ok(hash)
    }

    fn scan_all(&self) -> Result<RArray, Error> {
        let dataset = self.dataset.borrow();
        let dataset = dataset.as_ref()
            .ok_or_else(|| Error::new(magnus::exception::runtime_error(), "Dataset not opened"))?;

        let batches: Vec<RecordBatch> = self.runtime.borrow_mut().block_on(async {
            let mut scanner = dataset.scan();
            let stream = scanner
                .try_into_stream()
                .await
                .map_err(|e| Error::new(magnus::exception::runtime_error(), e.to_string()))?;
            
            stream
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e| Error::new(magnus::exception::runtime_error(), e.to_string()))
        })?;

        let ruby = Ruby::get().unwrap();
        let result_array = ruby.ary_new();

        for batch in batches {
            let documents = convert_batch_to_ruby(&batch)?;
            for doc in documents {
                result_array.push(doc)?;
            }
        }

        Ok(result_array)
    }

    fn scan_limit(&self, limit: i64) -> Result<RArray, Error> {
        let dataset = self.dataset.borrow();
        let dataset = dataset.as_ref()
            .ok_or_else(|| Error::new(magnus::exception::runtime_error(), "Dataset not opened"))?;

        let batches: Vec<RecordBatch> = self.runtime.borrow_mut().block_on(async {
            let mut scanner = dataset.scan();
            scanner.limit(Some(limit), None);
            
            let stream = scanner
                .try_into_stream()
                .await
                .map_err(|e| Error::new(magnus::exception::runtime_error(), e.to_string()))?;
            
            stream
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e| Error::new(magnus::exception::runtime_error(), e.to_string()))
        })?;

        let ruby = Ruby::get().unwrap();
        let result_array = ruby.ary_new();

        for batch in batches {
            let documents = convert_batch_to_ruby(&batch)?;
            for doc in documents {
                result_array.push(doc)?;
            }
        }

        Ok(result_array)
    }
}

fn build_arrow_schema(schema_hash: RHash) -> Result<ArrowSchema, Error> {
    let mut fields = Vec::new();

    schema_hash.foreach(|key: Symbol, value: Value| {
        let field_name = key.name()?.to_string();
        
        let data_type = if value.is_kind_of(magnus::class::hash()) {
            let hash = RHash::from_value(value)
                .ok_or_else(|| Error::new(magnus::exception::arg_error(), "Invalid hash value"))?;
            let type_str: String = hash.fetch(Symbol::new("type"))?;
            
            match type_str.as_str() {
                "vector" => {
                    let dimension: i32 = hash.fetch(Symbol::new("dimension"))?;
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Float32, true)),
                        dimension,
                    )
                }
                _ => return Err(Error::new(
                    magnus::exception::arg_error(),
                    format!("Unknown field type: {}", type_str)
                ))
            }
        } else {
            let type_str = String::try_convert(value)?;
            match type_str.as_str() {
                "string" => DataType::Utf8,
                "float32" => DataType::Float32,
                "float64" => DataType::Float64,
                "int32" => DataType::Int32,
                "int64" => DataType::Int64,
                "boolean" => DataType::Boolean,
                _ => return Err(Error::new(
                    magnus::exception::arg_error(),
                    format!("Unknown field type: {}", type_str)
                ))
            }
        };

        fields.push(Field::new(field_name, data_type, true));
        Ok(ForEach::Continue)
    })?;

    Ok(ArrowSchema::new(fields))
}

fn build_record_batch(
    data: RArray,
    schema: &ArrowSchema,
) -> Result<RecordBatch, Error> {
    let mut columns: HashMap<String, Vec<Option<String>>> = HashMap::new();
    let mut float_columns: HashMap<String, Vec<Option<f32>>> = HashMap::new();
    
    for field in schema.fields() {
        match field.data_type() {
            DataType::Utf8 => {
                columns.insert(field.name().to_string(), Vec::new());
            }
            DataType::Float32 => {
                float_columns.insert(field.name().to_string(), Vec::new());
            }
            _ => {}
        }
    }

    for item in data.into_iter() {
        let item = RHash::try_convert(item)?;
        for field in schema.fields() {
            let key = Symbol::new(field.name());
            let value: Value = item.fetch(key)
                .or_else(|_| {
                    // Try with string key  
                    item.fetch(field.name().as_str())
                })?;
            
            match field.data_type() {
                DataType::Utf8 => {
                    if value.is_nil() {
                        columns.get_mut(field.name()).unwrap().push(None);
                    } else {
                        let s = String::try_convert(value)?;
                        columns.get_mut(field.name()).unwrap().push(Some(s));
                    }
                }
                DataType::Float32 => {
                    if value.is_nil() {
                        float_columns.get_mut(field.name()).unwrap().push(None);
                    } else {
                        let f = f64::try_convert(value)?;
                        float_columns.get_mut(field.name()).unwrap().push(Some(f as f32));
                    }
                }
                _ => {}
            }
        }
    }

    let mut arrays: Vec<ArrayRef> = Vec::new();
    
    for field in schema.fields() {
        let array: ArrayRef = match field.data_type() {
            DataType::Utf8 => {
                let values = columns.get(field.name()).unwrap();
                Arc::new(StringArray::from(values.clone()))
            }
            DataType::Float32 => {
                let values = float_columns.get(field.name()).unwrap();
                Arc::new(Float32Array::from(values.clone()))
            }
            _ => return Err(Error::new(
                magnus::exception::runtime_error(),
                format!("Unsupported data type: {:?}", field.data_type())
            ))
        };
        
        arrays.push(array);
    }

    RecordBatch::try_new(Arc::new(schema.clone()), arrays)
        .map_err(|e| Error::new(magnus::exception::runtime_error(), e.to_string()))
}

fn convert_batch_to_ruby(batch: &RecordBatch) -> Result<Vec<RHash>, Error> {
    let ruby = Ruby::get().unwrap();
    let mut documents = Vec::new();
    
    let num_rows = batch.num_rows();
    let schema = batch.schema();
    
    for row_idx in 0..num_rows {
        let doc = ruby.hash_new();
        
        for (col_idx, field) in schema.fields().iter().enumerate() {
            let column = batch.column(col_idx);
            let key = Symbol::new(field.name());
            
            match field.data_type() {
                DataType::Utf8 => {
                    let array = column.as_any().downcast_ref::<StringArray>()
                        .ok_or_else(|| Error::new(magnus::exception::runtime_error(), "Failed to cast to StringArray"))?;
                    
                    if array.is_null(row_idx) {
                        doc.aset(key, ruby.qnil())?;
                    } else {
                        doc.aset(key, array.value(row_idx))?;
                    }
                }
                DataType::Float32 => {
                    let array = column.as_any().downcast_ref::<Float32Array>()
                        .ok_or_else(|| Error::new(magnus::exception::runtime_error(), "Failed to cast to Float32Array"))?;
                    
                    if array.is_null(row_idx) {
                        doc.aset(key, ruby.qnil())?;
                    } else {
                        doc.aset(key, array.value(row_idx))?;
                    }
                }
                _ => {
                    // Skip unsupported types for now
                }
            }
        }
        
        documents.push(doc);
    }
    
    Ok(documents)
}

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
    
    Ok(())
}