# Lancelot - Project Instructions

You are working on lancelot, a Ruby gem that provides native bindings to the Lance columnar data format through Rust and Magnus.

## Project Context

Lancelot is part of a Ruby-native NLP/ML ecosystem:
- **lance**: Rust crate providing columnar storage with vector and text search
- **lancelot**: THIS PROJECT - Ruby bindings to Lance via Magnus
- **red-candle**: Ruby gem providing LLMs, embeddings, and rerankers
- **candle-agent**: Future agent capabilities for the ecosystem

## Core Architecture

### Ruby-Rust Bridge
- Uses Magnus 0.7 for Ruby-Rust interop
- RefCell pattern for interior mutability
- Embedded Tokio runtime for async Lance operations
- Clean separation between Ruby API and Rust implementation

### Key Components
- `lib/lancelot/dataset.rb`: Ruby-idiomatic API layer
- `ext/lancelot/src/dataset.rs`: Core Lance operations
- `ext/lancelot/src/schema.rs`: Schema building and type mapping
- `ext/lancelot/src/conversion.rs`: Ruby-Rust data conversion

## Design Principles

1. **Ruby-First API**: Make it feel native to Ruby developers
   - Use symbols for type names (`:string`, `:vector`)
   - Support operator overloading (`<<` for append)
   - Include Enumerable for iteration
   - Return Ruby hashes/arrays, not foreign objects

2. **Schema-First Design**: Lance requires schemas at creation
   - Clear schema definition API
   - Helpful error messages for type mismatches
   - Support all Arrow types Lance supports

3. **Performance Without Complexity**: Hide async/columnar details
   - Embed Tokio runtime, don't expose it
   - Convert RecordBatches to Ruby transparently
   - Efficient batch operations when possible

4. **Error Handling**: Rust errors become Ruby exceptions
   - Use Magnus's error conversion
   - Provide context in error messages
   - Never panic in Rust code

## Current Features

### Implemented
- Dataset creation with schema
- CRUD operations (add, update, delete, get)
- Vector search with ANN indices
- Full-text search with inverted indices
- Multi-column text search
- SQL-like filtering
- Enumerable support

### Not Yet Implemented
- Schema evolution
- Hybrid search (vector + text fusion)
- Streaming operations
- Transaction support
- Data versioning/time travel

## Development Guidelines

### Adding New Features

1. **Start with the Ruby API**: Design how it should feel in Ruby first
2. **Implement in Rust**: Keep the Rust layer focused on Lance operations
3. **Type Conversion**: Use conversion.rs patterns for new types
4. **Testing**: Add Ruby tests in spec/ and Rust tests in src/

### Magnus Best Practices

1. **Memory Management**:
   ```rust
   #[magnus::wrap(class = "Lancelot::Dataset", free_immediately, size)]
   ```

2. **Method Definition**:
   ```rust
   class.define_method("method_name", method!(LancelotDataset::method_name, arity))?;
   ```

3. **Error Handling**:
   ```rust
   pub fn operation(&self) -> Result<Value, Error> {
       self.with_dataset(|dataset| {
           // Lance operation
       }).map_err(|e| Error::new(exception::runtime_error(), e.to_string()))
   }
   ```

4. **Async Operations**:
   ```rust
   self.with_runtime(|runtime| {
       runtime.block_on(async {
           // Async Lance operation
       })
   })
   ```

### Type Mappings

Ruby → Arrow/Lance:
- `:string` → Utf8
- `:integer` → Int64
- `:float` → Float64
- `:vector` → FixedSizeList with dimension
- `:boolean` → Bool
- `:date` → Date32
- `:datetime` → Timestamp

### Testing

- Ruby specs use RSpec
- Test both successful operations and error cases
- Use temporary directories for test datasets
- Clean up resources in after blocks

## Common Tasks

### Adding a New Search Method
1. Define Ruby API in dataset.rb
2. Add Rust implementation in dataset.rs
3. Handle type conversion if needed
4. Add index support if applicable
5. Write comprehensive tests

### Exposing Lance Features
1. Check Lance API documentation
2. Design Ruby-idiomatic wrapper
3. Consider if it needs async handling
4. Implement with proper error handling
5. Document with YARD comments

## Integration Points

### With Red-Candle
- Lancelot stores embeddings from red-candle
- Vector dimensions must match model output
- Consider batch operations for efficiency

### Future: Hybrid Search
- Will combine vector and text search results
- RRF (Reciprocal Rank Fusion) planned
- Consider implementing in Ruby first

## Performance Considerations

1. **Batch Operations**: Always prefer batch over individual ops
2. **Index Building**: Build indices after bulk loading
3. **Memory Usage**: Lance uses memory mapping efficiently
4. **Ruby GC**: Use free_immediately for deterministic cleanup

## Debugging Tips

1. **Rust Panics**: Use `RUST_BACKTRACE=1` for stack traces
2. **Lance Logs**: Set `RUST_LOG=lance=debug`
3. **Ruby-Rust Bridge**: Check type conversions first
4. **Async Issues**: Ensure operations run on the runtime

## Release Process

1. Update version.rb
2. Run full test suite
3. Build gem locally and test
4. Update CHANGELOG.md
5. Tag release and push
6. `rake release` to publish

Remember: The goal is to make Lance's power accessible to Ruby developers without them needing to understand columnar formats, Rust, or async programming.