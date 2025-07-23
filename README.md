# Lancelot

Ruby bindings for [Lance](https://github.com/lancedb/lance), a modern columnar data format for ML. Lancelot provides a Ruby-native interface to Lance, enabling efficient storage and search of multimodal data including text, vectors, and more.

## Features (Planned)

- **Columnar Storage**: Efficient storage using Lance's columnar format
- **Full-Text Search**: Built-in full-text search capabilities
- **Vector Search**: Similarity search for embeddings and vectors
- **Hybrid Search**: Combine text and vector search with RRF and other fusion methods
- **Multimodal Support**: Store and search across different data types
- **Ruby-Native API**: Idiomatic Ruby interface that feels natural

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

# Create or open a dataset
dataset = Lancelot::Dataset.create("path/to/dataset")

# Add documents with text and vectors
dataset.add_documents([
  { 
    text: "Ruby is a dynamic programming language", 
    vector: [0.1, 0.2, 0.3, ...],
    metadata: { category: "programming" }
  }
])

# Search using text, vectors, or both
results = dataset.search(
  text: "programming language",
  limit: 10
)
```

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To compile the Rust extension:
```bash
bundle exec rake compile
```

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/assaydepot/lancelot. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [code of conduct](https://github.com/assaydepot/lancelot/blob/main/CODE_OF_CONDUCT.md).

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

## Code of Conduct

Everyone interacting in the Lancelot project's codebases, issue trackers, chat rooms and mailing lists is expected to follow the [code of conduct](https://github.com/assaydepot/lancelot/blob/main/CODE_OF_CONDUCT.md).