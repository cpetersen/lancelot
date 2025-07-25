#!/usr/bin/env ruby

require 'bundler/setup'
require 'lancelot'
require 'tmpdir'

Dir.mktmpdir do |dir|
  dataset_path = File.join(dir, "text_dataset")
  
  puts "Creating dataset with text data..."
  
  # Create a dataset with text fields
  dataset = Lancelot::Dataset.create(dataset_path, schema: {
    title: :string,
    content: :string,
    category: :string,
    year: :int64
  })
  
  # Sample documents
  documents = [
    { 
      title: "Introduction to Ruby", 
      content: "Ruby is a dynamic, object-oriented programming language known for its simplicity",
      category: "tutorial",
      year: 2023
    },
    { 
      title: "Advanced Ruby Techniques", 
      content: "Learn advanced Ruby programming patterns and metaprogramming techniques",
      category: "advanced",
      year: 2024
    },
    { 
      title: "Python for Beginners", 
      content: "Python is a versatile programming language great for beginners",
      category: "tutorial",
      year: 2023
    },
    { 
      title: "Python Data Science", 
      content: "Using Python for data analysis, machine learning, and scientific computing",
      category: "data-science",
      year: 2024
    },
    { 
      title: "JavaScript Fundamentals", 
      content: "JavaScript is the programming language of the web",
      category: "tutorial",
      year: 2022
    },
    { 
      title: "Rust Systems Programming", 
      content: "Rust provides memory safety and high performance for systems programming",
      category: "systems",
      year: 2024
    }
  ]
  
  # Add documents
  dataset.add_documents(documents)
  puts "Added #{dataset.count} documents\n\n"
  
  # Test 1: Basic text search
  puts "=== Basic Text Search ==="
  puts "Searching for 'Ruby' in content:"
  results = dataset.text_search("Ruby", column: "content", limit: 5)
  results.each do |doc|
    puts "  - #{doc[:title]}: #{doc[:content][0..60]}..."
  end
  
  # Test 2: Search in title
  puts "\n\nSearching for 'Python' in title:"
  results = dataset.text_search("Python", column: "title", limit: 5)
  results.each do |doc|
    puts "  - #{doc[:title]}"
  end
  
  # Test 3: Case-insensitive search
  puts "\n\nSearching for 'programming' in content:"
  results = dataset.text_search("programming", column: "content", limit: 10)
  results.each do |doc|
    puts "  - #{doc[:title]}"
  end
  
  # Test 4: SQL-like filtering with where
  puts "\n\n=== SQL-like Filtering ==="
  puts "Finding tutorials from 2023:"
  results = dataset.where("category = 'tutorial' AND year = 2023")
  results.each do |doc|
    puts "  - #{doc[:title]} (#{doc[:year]})"
  end
  
  # Test 5: Complex filter
  puts "\n\nFinding advanced or data-science content from 2024:"
  results = dataset.where("(category = 'advanced' OR category = 'data-science') AND year = 2024")
  results.each do |doc|
    puts "  - #{doc[:title]} - #{doc[:category]}"
  end
  
  # Test 6: LIKE pattern matching
  puts "\n\nFinding content with 'data' in category:"
  results = dataset.where("category LIKE '%data%'")
  results.each do |doc|
    puts "  - #{doc[:title]} - #{doc[:category]}"
  end
  
  # Test 7: Combining text search with limit
  puts "\n\nSearching for 'language' with limit 2:"
  results = dataset.text_search("language", column: "content", limit: 2)
  results.each do |doc|
    puts "  - #{doc[:title]}"
  end
end

puts "\nDone!"