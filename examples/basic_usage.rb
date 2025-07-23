#!/usr/bin/env ruby

require "bundler/setup"
require "lancelot"
require "tmpdir"

# Create a temporary directory for our dataset
Dir.mktmpdir do |dir|
  dataset_path = File.join(dir, "my_dataset")
  
  puts "Creating dataset at: #{dataset_path}"
  
  # Create a dataset with a schema
  dataset = Lancelot::Dataset.create(dataset_path, schema: {
    text: :string,
    score: :float32
  })
  
  # Add documents
  dataset.add_documents([
    { text: "Ruby is a dynamic programming language", score: 0.95 },
    { text: "Python is great for data science", score: 0.88 }
  ])
  
  # Or use the << operator
  dataset << { text: "JavaScript runs everywhere", score: 0.92 }
  
  # Get the count
  puts "Document count: #{dataset.count}"
  
  # Get the schema
  puts "Schema: #{dataset.schema.inspect}"
  
  # Retrieve documents
  puts "\nRetrieving documents:"
  puts "First document: #{dataset.first.inspect}"
  puts "First 2 documents: #{dataset.first(2).map { |d| d[:text] }.join(', ')}"
  
  # Use Enumerable methods
  puts "\nUsing Enumerable methods:"
  texts = dataset.map { |doc| doc[:text] }
  puts "All texts: #{texts.join(', ')}"
  
  high_scores = dataset.select { |doc| doc[:score] > 0.9 }
  puts "High scoring documents: #{high_scores.map { |d| d[:text] }.join(', ')}"
  
  # Open an existing dataset
  dataset2 = Lancelot::Dataset.open(dataset_path)
  puts "\nOpened dataset has #{dataset2.count} documents"
end

puts "Done!"