#!/usr/bin/env ruby

require 'bundler/setup'
require 'lancelot'
require 'tmpdir'

Dir.mktmpdir do |dir|
  dataset_path = File.join(dir, "articles_dataset")
  
  puts "Creating dataset with article data..."
  
  # Create a dataset with multiple text fields
  dataset = Lancelot::Dataset.create(dataset_path, schema: {
    title: :string,
    content: :string,
    category: :string,
    author: :string,
    year: :int64,
    tags: :string
  })
  
  # Sample articles
  articles = [
    { 
      title: "Getting Started with Ruby on Rails", 
      content: "Ruby on Rails is a powerful web framework that makes building applications fast and enjoyable. It follows the MVC pattern and emphasizes convention over configuration.",
      category: "web development",
      author: "Alice Johnson",
      year: 2024,
      tags: "ruby rails web mvc framework"
    },
    { 
      title: "Advanced Ruby Metaprogramming Techniques", 
      content: "Ruby's metaprogramming capabilities allow you to write code that writes code. Learn about method_missing, define_method, and dynamic class creation.",
      category: "programming",
      author: "Bob Smith",
      year: 2024,
      tags: "ruby metaprogramming advanced dynamic"
    },
    { 
      title: "Building RESTful APIs with Rails", 
      content: "Learn how to build robust RESTful APIs using Ruby on Rails. We'll cover routing, controllers, serialization, and authentication.",
      category: "web development",
      author: "Alice Johnson",
      year: 2023,
      tags: "ruby rails api rest web services"
    },
    { 
      title: "Python vs Ruby: A Comprehensive Comparison", 
      content: "Both Python and Ruby are dynamic, interpreted languages. This article compares their syntax, performance, ecosystem, and use cases.",
      category: "programming",
      author: "Charlie Davis",
      year: 2024,
      tags: "python ruby comparison languages programming"
    },
    { 
      title: "Machine Learning with Python", 
      content: "Python has become the de facto language for machine learning. Explore popular libraries like scikit-learn, TensorFlow, and PyTorch.",
      category: "data science",
      author: "David Lee",
      year: 2024,
      tags: "python machine learning ml ai data science"
    },
    { 
      title: "Rust for Systems Programming", 
      content: "Rust provides memory safety without garbage collection. Learn how Rust is revolutionizing systems programming with its ownership model.",
      category: "systems",
      author: "Eve Wilson",
      year: 2023,
      tags: "rust systems programming memory safety performance"
    }
  ]
  
  # Add articles
  dataset.add_documents(articles)
  puts "Added #{dataset.count} articles\n\n"
  
  # Create text indices on multiple columns
  puts "Creating text indices..."
  dataset.create_text_index("title")
  dataset.create_text_index("content")
  dataset.create_text_index("tags")
  puts "Text indices created\n\n"
  
  # Test 1: Single column full-text search
  puts "=== Single Column Full-Text Search ==="
  
  puts "\nSearching for 'ruby' in content:"
  results = dataset.text_search("ruby", column: "content", limit: 5)
  results.each do |doc|
    puts "  - #{doc[:title]}"
    puts "    #{doc[:content][0..80]}..."
  end
  
  # Test 2: Search in title
  puts "\n\nSearching for 'python' in title:"
  results = dataset.text_search("python", column: "title", limit: 5)
  results.each do |doc|
    puts "  - #{doc[:title]} (#{doc[:year]})"
  end
  
  # Test 3: Search in tags
  puts "\n\nSearching for 'programming' in tags:"
  results = dataset.text_search("programming", column: "tags", limit: 5)
  results.each do |doc|
    puts "  - #{doc[:title]}"
    puts "    Tags: #{doc[:tags]}"
  end
  
  # Test 4: Multi-column search
  puts "\n\n=== Multi-Column Full-Text Search ==="
  
  puts "\nSearching for 'ruby' across title and content:"
  results = dataset.text_search("ruby", columns: ["title", "content"], limit: 10)
  results.each do |doc|
    puts "  - #{doc[:title]} by #{doc[:author]}"
  end
  
  # Test 5: Complex multi-word queries
  puts "\n\nSearching for 'machine learning' across all text fields:"
  results = dataset.text_search("machine learning", columns: ["title", "content", "tags"], limit: 5)
  results.each do |doc|
    puts "  - #{doc[:title]}"
    puts "    Category: #{doc[:category]}"
  end
  
  # Test 6: Combining with SQL filters
  puts "\n\n=== Combining Full-Text Search with Filters ==="
  
  # First do a text search, then filter by year
  puts "\nArticles about 'programming' from 2024:"
  all_results = dataset.text_search("programming", column: "content", limit: 20)
  filtered = all_results.select { |doc| doc[:year] == 2024 }
  filtered.each do |doc|
    puts "  - #{doc[:title]} (#{doc[:year]})"
  end
  
  # Or use SQL filter for category
  puts "\n\nWeb development articles:"
  results = dataset.where("category = 'web development'")
  results.each do |doc|
    puts "  - #{doc[:title]}"
  end
end

puts "\nDone!"