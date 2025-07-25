#!/usr/bin/env ruby

require 'bundler/setup'
require 'lancelot'
require 'red-candle'
require 'tmpdir'

Dir.mktmpdir do |dir|
  dataset_path = File.join(dir, "embeddings.lance")
  
  puts "Creating dataset at: #{dataset_path}"
  
  # Create a dataset with text and embedding columns
  dataset = Lancelot::Dataset.create(dataset_path, schema: {
    text: :string,
    embedding: { type: "vector", dimension: 768 }
  })
  
  # Initialize the embedding model
  embedding_model = Candle::EmbeddingModel.new
  puts "Using embedding model: jinaai/jina-embeddings-v2-base-en"
  
  # Sample documents
  documents = [
    "Ruby is a dynamic, object-oriented programming language",
    "Python is great for data science and machine learning",
    "JavaScript runs in browsers and on servers with Node.js",
    "Rust provides memory safety without garbage collection",
    "Go makes concurrent programming easy with goroutines",
    "Java is widely used in enterprise applications",
    "C++ offers high performance and low-level control",
    "TypeScript adds static typing to JavaScript",
  ]
  
  # Add documents with embeddings
  puts "\nAdding documents..."
  documents.each do |text|
    embedding = embedding_model.embedding(text)
    
    # Convert tensor to array (remove batch dimension)
    embedding_array = embedding.squeeze(0).to_a
    
    dataset.add_documents([
      { text: text, embedding: embedding_array }
    ])
    
    puts "  Added: #{text[0..50]}..."
  end
  
  puts "\nTotal documents: #{dataset.count}"
  
  # Create vector index
  puts "\nCreating vector index..."
  dataset.create_vector_index("embedding")
  
  # Perform vector search
  query = "Which languages are good for systems programming?"
  puts "\nSearching for: '#{query}'"
  
  # Generate embedding for query
  query_embedding = embedding_model.embedding(query)
  query_array = query_embedding.squeeze(0).to_a
  
  # Search for similar documents
  results = dataset.vector_search(query_array, column: "embedding", limit: 3)
  
  puts "\nTop 3 most similar documents:"
  results.each_with_index do |doc, i|
    puts "#{i + 1}. #{doc[:text]}"
  end
  
  # Another search
  query2 = "dynamic typing and interpreted languages"
  puts "\n\nSearching for: '#{query2}'"
  
  query_embedding2 = embedding_model.embedding(query2)
  query_array2 = query_embedding2.squeeze(0).to_a
  
  similar = dataset.nearest_neighbors(query_array2, k: 3, column: "embedding")
  
  puts "\nTop 3 nearest neighbors:"
  similar.each_with_index do |doc, i|
    puts "#{i + 1}. #{doc[:text]}"
  end
end

puts "\nDone!"