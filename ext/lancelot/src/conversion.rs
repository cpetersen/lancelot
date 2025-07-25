use magnus::{Error, Ruby, RHash, RArray, Symbol, Value, TryConvert, value::ReprValue};
use arrow_schema::{DataType, Schema as ArrowSchema};
use arrow_array::{RecordBatch, StringArray, Float32Array, ArrayRef, Array, FixedSizeListArray};
use std::collections::HashMap;
use std::sync::Arc;

pub fn build_record_batch(
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

pub fn convert_batch_to_ruby(batch: &RecordBatch) -> Result<Vec<RHash>, Error> {
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