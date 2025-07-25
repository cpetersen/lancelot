# Lancelot

Ruby bindings for [Lance](https://github.com/lancedb/lance), a modern columnar data format for ML. Lancelot provides a Ruby-native interface to Lance, enabling efficient storage and search of multimodal data including text, vectors, and more.

## Features

### Implemented
- **Dataset Creation**: Create Lance datasets with schemas
- **Data Storage**: Add documents to datasets  
- **Document Retrieval**: Read documents from datasets with enumerable support
- **Vector Search**: Create vector indices and perform similarity search
- **Schema Support**: Define schemas with string, float32, and vector types
- **Row Counting**: Get the number of rows in a dataset

### Planned

- **Full-Text Search**: Built-in full-text search capabilities  
- **Hybrid Search**: Combine text and vector search with RRF and other fusion methods
- **Multimodal Support**: Store and search across different data types beyond text and vectors
- **Schema Evolution**: Add new columns to existing datasets without rewriting data

## Installation

Install the gem and add to the application's Gemfile by executing:

```bash
bundle add lancelot
```

If bundler is not being used to manage dependencies, install the gem by executing:

```bash
gem install lancelot
```

## Usage

```ruby
require 'lancelot'

# Create a dataset with a schema including vectors
dataset = Lancelot::Dataset.create("path/to/dataset", schema: {
  text: :string,
  score: :float32,
  embedding: { type: "vector", dimension: 128 }
})

# Add documents with embeddings
dataset.add_documents([
  { text: "Ruby is a dynamic programming language", score: 0.95, embedding: [0.1, 0.2, ...] },
  { text: "Python is great for data science", score: 0.88, embedding: [0.2, 0.3, ...] }
])

# Or use the << operator
dataset << { text: "JavaScript runs everywhere", score: 0.92, embedding: [0.3, 0.4, ...] }

# Open an existing dataset
dataset = Lancelot::Dataset.open("path/to/dataset")

# Get the count
puts dataset.count  # => 3

# Get the schema
puts dataset.schema  # => { text: "string", score: "float32" }

# Retrieve documents
dataset.all           # => Returns all documents
dataset.first         # => Returns first document
dataset.first(2)      # => Returns first 2 documents

# Enumerable support
dataset.each { |doc| puts doc[:text] }
dataset.map { |doc| doc[:score] }
dataset.select { |doc| doc[:score] > 0.9 }

# Vector search
dataset.create_vector_index("embedding")  # Create vector index
results = dataset.vector_search([0.15, 0.25, ...], column: "embedding", limit: 5)  # Find 5 nearest neighbors

# Or use the nearest_neighbors alias
similar = dataset.nearest_neighbors([0.1, 0.2, ...], k: 10, column: "embedding")

# Full-text search with inverted indices
# First create text indices on the columns you want to search
dataset.create_text_index("title")
dataset.create_text_index("content")
dataset.create_text_index("tags")

# Single column search
results = dataset.text_search("ruby programming", column: "content", limit: 10)

# Multi-column search
results = dataset.text_search("machine learning", columns: ["title", "content"], limit: 10)

# SQL-like filtering (uses Lance's SQL engine, not full-text indices)
results = dataset.where("score > 0.9")
results = dataset.where("category = 'tutorial' AND year >= 2023", limit: 5)
```

### Full-Text Search

Lancelot supports Lance's full-text search capabilities with inverted indices:

```ruby
# Create indices before searching
dataset.create_text_index("title")
dataset.create_text_index("content")

# Search a single column
results = dataset.text_search("ruby", column: "title")

# Search multiple columns (returns union of results)
results = dataset.text_search("programming", columns: ["title", "content", "tags"])

# The underlying Lance engine provides:
# - BM25 scoring for relevance ranking
# - Tokenization with language support
# - Case-insensitive search
# - Multi-word queries
```

**Note**: Full-text search requires creating inverted indices first. For simple pattern matching without indices, use SQL-like filtering with `where`.

**Current Limitations:**
- Schema must be defined when creating a dataset
- Schema evolution is not yet implemented (Lance supports it, but our bindings don't expose it yet)
- Hybrid search (RRF) is not yet implemented
- Supported field types: string, float32, float64, int32, int64, boolean, and fixed-size vectors

**Note on Lance's Schema Flexibility:**
Lance itself supports schema evolution - you can add new columns without rewriting data. However, our current Ruby bindings have simplified this and require an upfront schema. This will be improved in future releases to expose Lance's full flexibility.

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To compile the Rust extension:
```bash
bundle exec rake compile
```

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/cpetersen/lancelot. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [code of conduct](https://github.com/cpetersen/lancelot/blob/main/CODE_OF_CONDUCT.md).

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

## Code of Conduct

Everyone interacting in the Lancelot project's codebases, issue trackers, chat rooms and mailing lists is expected to follow the [code of conduct](https://github.com/cpetersen/lancelot/blob/main/CODE_OF_CONDUCT.md).
