#!/usr/bin/env ruby

require "bundler/setup"
require "lancelot"
require "tmpdir"

# Simple function to generate random embeddings
def generate_embedding(text)
  # In real applications, you'd use an actual embedding model
  # This is just for demonstration
  text.bytes.take(128).map { |b| b / 255.0 } + [0.0] * (128 - text.bytes.take(128).length)
end

# Create a temporary directory for our dataset
Dir.mktmpdir do |dir|
  dataset_path = File.join(dir, "vector_dataset")
  
  puts "Creating dataset with vector support at: #{dataset_path}"
  
  # Create a dataset with vector schema
  dataset = Lancelot::Dataset.create(dataset_path, schema: {
    text: :string,
    score: :float32,
    vector: { type: "vector", dimension: 128 }
  })
  
  # Sample documents about programming languages
  documents = [
    { 
      text: "Ruby is a dynamic, object-oriented programming language",
      score: 0.95
    },
    { 
      text: "Python is great for data science and machine learning",
      score: 0.92
    },
    { 
      text: "JavaScript runs in browsers and on servers with Node.js",
      score: 0.88
    },
    { 
      text: "Rust provides memory safety without garbage collection",
      score: 0.91
    },
    { 
      text: "Go makes concurrent programming easy with goroutines",
      score: 0.89
    },
    { 
      text: "Java is widely used in enterprise applications",
      score: 0.85
    },
    { 
      text: "C++ offers high performance and low-level control",
      score: 0.90
    },
    { 
      text: "TypeScript adds static typing to JavaScript",
      score: 0.87
    }
  ]
  
  # Add embeddings to documents
  documents_with_embeddings = documents.map do |doc|
    doc.merge(vector: generate_embedding(doc[:text]))
  end
  
  # Add documents to dataset
  puts "Adding documents to dataset..."
  dataset.add_documents(documents_with_embeddings)
  puts "Added #{dataset.count} documents"
  
  # Create vector index
  puts "\nCreating vector index..."
  dataset.create_vector_index("vector")
  
  # Perform vector search
  puts "\nSearching for documents similar to 'dynamic programming languages'..."
  query_embedding = generate_embedding("dynamic programming languages")
  
  results = dataset.vector_search(query_embedding, column: "vector", limit: 3)
  
  puts "\nTop 3 most similar documents:"
  results.each_with_index do |doc, i|
    puts "#{i + 1}. #{doc[:text]} (score: #{doc[:score]})"
    puts
  end
  
  # Search with nearest_neighbors alias
  puts "Searching for documents similar to 'memory safety and performance'..."
  query_embedding2 = generate_embedding("memory safety and performance")
  
  similar = dataset.nearest_neighbors(query_embedding2, k: 3, column: "vector")
  
  puts "\nTop 3 nearest neighbors:"
  similar.each_with_index do |doc, i|
    puts "#{i + 1}. #{doc[:text]} (score: #{doc[:score]})"
    puts
  end
end

puts "\nDone!"