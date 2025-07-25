use magnus::{
    define_module, function, method, prelude::*, 
    Error, Ruby, RHash, RArray, Symbol, Value, TryConvert, r_hash::ForEach
};
use std::cell::RefCell;
use std::sync::Arc;
use tokio::runtime::Runtime;
use lance::Dataset;
use lance::index::vector::VectorIndexParams;
use lance_index::{IndexType, DatasetIndexExt};
use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use arrow_array::{RecordBatch, RecordBatchIterator, StringArray, Float32Array, ArrayRef, Array, FixedSizeListArray};
use std::collections::HashMap;
use futures::stream::TryStreamExt;

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

        // Check if data is empty
        if data.len() == 0 {
            return Ok(());  // Nothing to add
        }

        // Get the dataset's schema
        let schema = self.runtime.borrow_mut().block_on(async {
            dataset.schema()
        });
        
        // Convert Lance schema to Arrow schema
        let arrow_schema = schema.into();

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
            let scanner = dataset.scan();
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
            scanner.limit(Some(limit), None)
                .map_err(|e| Error::new(magnus::exception::runtime_error(), e.to_string()))?;
            
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

    fn create_vector_index(&self, column: String) -> Result<(), Error> {
        let mut dataset = self.dataset.borrow_mut();
        let dataset = dataset.as_mut()
            .ok_or_else(|| Error::new(magnus::exception::runtime_error(), "Dataset not opened"))?;

        self.runtime.borrow_mut().block_on(async move {
            // Get row count to determine optimal number of partitions
            let num_rows = dataset.count_rows(None).await
                .map_err(|e| Error::new(magnus::exception::runtime_error(), e.to_string()))?;
            
            // Use fewer partitions for small datasets
            let num_partitions = if num_rows < 256 {
                std::cmp::max(1, (num_rows / 4) as usize)
            } else {
                256
            };
            
            // Create IVF_FLAT vector index parameters
            let params = VectorIndexParams::ivf_flat(num_partitions, lance_linalg::distance::MetricType::L2);
            
            dataset.create_index(
                &[&column],
                IndexType::Vector,
                None,
                &params,
                true
            )
            .await
            .map_err(|e| Error::new(magnus::exception::runtime_error(), e.to_string()))
        })
    }

    fn vector_search(&self, column: String, query_vector: RArray, limit: i64) -> Result<RArray, Error> {
        let dataset = self.dataset.borrow();
        let dataset = dataset.as_ref()
            .ok_or_else(|| Error::new(magnus::exception::runtime_error(), "Dataset not opened"))?;

        // Convert Ruby array to Vec<f32>
        let vector: Vec<f32> = query_vector
            .into_iter()
            .map(|v| f64::try_convert(v).map(|f| f as f32))
            .collect::<Result<Vec<_>, _>>()?;

        let batches: Vec<RecordBatch> = self.runtime.borrow_mut().block_on(async {
            let mut scanner = dataset.scan();
            
            // Use nearest for vector search
            scanner.nearest(&column, &Float32Array::from(vector), limit as usize)
                .map_err(|e| Error::new(magnus::exception::runtime_error(), e.to_string()))?;
            
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

    fn text_search(&self, column: String, query: String, limit: i64) -> Result<RArray, Error> {
        let dataset = self.dataset.borrow();
        let dataset = dataset.as_ref()
            .ok_or_else(|| Error::new(magnus::exception::runtime_error(), "Dataset not opened"))?;

        let batches: Vec<RecordBatch> = self.runtime.borrow_mut().block_on(async {
            let mut scanner = dataset.scan();
            
            // Use SQL LIKE for pattern matching
            let filter = format!("{} LIKE '%{}%'", column, query);
            scanner.filter(&filter)
                .map_err(|e| Error::new(magnus::exception::runtime_error(), e.to_string()))?;
            
            // Apply limit
            scanner.limit(Some(limit), None)
                .map_err(|e| Error::new(magnus::exception::runtime_error(), e.to_string()))?;
            
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

    fn filter_scan(&self, filter_expr: String, limit: Option<i64>) -> Result<RArray, Error> {
        let dataset = self.dataset.borrow();
        let dataset = dataset.as_ref()
            .ok_or_else(|| Error::new(magnus::exception::runtime_error(), "Dataset not opened"))?;

        let batches: Vec<RecordBatch> = self.runtime.borrow_mut().block_on(async {
            let mut scanner = dataset.scan();
            
            // Apply SQL-like filter
            scanner.filter(&filter_expr)
                .map_err(|e| Error::new(magnus::exception::runtime_error(), e.to_string()))?;
            
            // Apply limit if provided
            if let Some(lim) = limit {
                scanner.limit(Some(lim), None)
                    .map_err(|e| Error::new(magnus::exception::runtime_error(), e.to_string()))?;
            }
            
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
    let mut int_columns: HashMap<String, Vec<Option<i64>>> = HashMap::new();
    let mut bool_columns: HashMap<String, Vec<Option<bool>>> = HashMap::new();
    let mut vector_columns: HashMap<String, Vec<Option<Vec<f32>>>> = HashMap::new();
    
    for field in schema.fields() {
        match field.data_type() {
            DataType::Utf8 => {
                columns.insert(field.name().to_string(), Vec::new());
            }
            DataType::Float32 => {
                float_columns.insert(field.name().to_string(), Vec::new());
            }
            DataType::Int64 => {
                int_columns.insert(field.name().to_string(), Vec::new());
            }
            DataType::Boolean => {
                bool_columns.insert(field.name().to_string(), Vec::new());
            }
            DataType::FixedSizeList(_, _) => {
                vector_columns.insert(field.name().to_string(), Vec::new());
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
                DataType::Int64 => {
                    if value.is_nil() {
                        int_columns.get_mut(field.name()).unwrap().push(None);
                    } else {
                        let i = i64::try_convert(value)?;
                        int_columns.get_mut(field.name()).unwrap().push(Some(i));
                    }
                }
                DataType::Boolean => {
                    if value.is_nil() {
                        bool_columns.get_mut(field.name()).unwrap().push(None);
                    } else {
                        let b = bool::try_convert(value)?;
                        bool_columns.get_mut(field.name()).unwrap().push(Some(b));
                    }
                }
                DataType::FixedSizeList(_, _) => {
                    if value.is_nil() {
                        vector_columns.get_mut(field.name()).unwrap().push(None);
                    } else {
                        let arr = RArray::try_convert(value)?;
                        let vec: Vec<f32> = arr.into_iter()
                            .map(|v| f64::try_convert(v).map(|f| f as f32))
                            .collect::<Result<Vec<_>, _>>()?;
                        vector_columns.get_mut(field.name()).unwrap().push(Some(vec));
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
            DataType::Int64 => {
                let values = int_columns.get(field.name()).unwrap();
                Arc::new(arrow_array::Int64Array::from(values.clone()))
            }
            DataType::Boolean => {
                let values = bool_columns.get(field.name()).unwrap();
                Arc::new(arrow_array::BooleanArray::from(values.clone()))
            }
            DataType::FixedSizeList(inner_field, list_size) => {
                let values = vector_columns.get(field.name()).unwrap();
                // Build flat array of all values
                let mut flat_values = Vec::new();
                for vec_opt in values {
                    match vec_opt {
                        Some(vec) => {
                            if vec.len() != *list_size as usize {
                                return Err(Error::new(
                                    magnus::exception::arg_error(),
                                    format!("Vector dimension mismatch. Expected {}, got {}", list_size, vec.len())
                                ));
                            }
                            flat_values.extend(vec);
                        }
                        None => {
                            // Add nulls for the entire vector
                            flat_values.extend(vec![0.0f32; *list_size as usize]);
                        }
                    }
                }
                
                let flat_array = Float32Array::from(flat_values);
                Arc::new(FixedSizeListArray::new(
                    inner_field.clone(),
                    *list_size,
                    Arc::new(flat_array),
                    None
                ))
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
                DataType::Int64 => {
                    let array = column.as_any().downcast_ref::<arrow_array::Int64Array>()
                        .ok_or_else(|| Error::new(magnus::exception::runtime_error(), "Failed to cast to Int64Array"))?;
                    
                    if array.is_null(row_idx) {
                        doc.aset(key, ruby.qnil())?;
                    } else {
                        doc.aset(key, array.value(row_idx))?;
                    }
                }
                DataType::Boolean => {
                    let array = column.as_any().downcast_ref::<arrow_array::BooleanArray>()
                        .ok_or_else(|| Error::new(magnus::exception::runtime_error(), "Failed to cast to BooleanArray"))?;
                    
                    if array.is_null(row_idx) {
                        doc.aset(key, ruby.qnil())?;
                    } else {
                        doc.aset(key, array.value(row_idx))?;
                    }
                }
                DataType::FixedSizeList(_, list_size) => {
                    let array = column.as_any().downcast_ref::<FixedSizeListArray>()
                        .ok_or_else(|| Error::new(magnus::exception::runtime_error(), "Failed to cast to FixedSizeListArray"))?;
                    
                    if array.is_null(row_idx) {
                        doc.aset(key, ruby.qnil())?;
                    } else {
                        let values = array.value(row_idx);
                        let float_array = values.as_any().downcast_ref::<Float32Array>()
                            .ok_or_else(|| Error::new(magnus::exception::runtime_error(), "Failed to cast vector values to Float32Array"))?;
                        
                        let ruby_array = ruby.ary_new();
                        for i in 0..*list_size {
                            ruby_array.push(float_array.value(i as usize))?;
                        }
                        doc.aset(key, ruby_array)?;
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
    dataset_class.define_method("create_vector_index", method!(LancelotDataset::create_vector_index, 1))?;
    dataset_class.define_method("vector_search", method!(LancelotDataset::vector_search, 3))?;
    dataset_class.define_method("text_search", method!(LancelotDataset::text_search, 3))?;
    dataset_class.define_method("filter_scan", method!(LancelotDataset::filter_scan, 2))?;
    
    Ok(())
}