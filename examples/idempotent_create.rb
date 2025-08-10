#!/usr/bin/env ruby
# Demonstrates idempotent dataset creation with open_or_create

require 'bundler/setup'
require 'lancelot'
require 'fileutils'

dataset_path = "words"

puts "="*60
puts "Idempotent Dataset Creation Demo"
puts "="*60

schema = {
  text: :string,
  embedding: { type: "vector", dimension: 768 }
}

# First call - will CREATE the dataset
puts "\n1. First call to open_or_create (should create)..."
dataset = Lancelot::Dataset.open_or_create(dataset_path, schema: schema)
puts "   Dataset opened/created. Current count: #{dataset.count}"

# Add some data
dataset.add_documents([
  { text: "hello", embedding: Array.new(768) { rand } },
  { text: "world", embedding: Array.new(768) { rand } }
])
puts "   Added 2 documents. New count: #{dataset.count}"

# Second call - will OPEN the existing dataset
puts "\n2. Second call to open_or_create (should open existing)..."
dataset2 = Lancelot::Dataset.open_or_create(dataset_path, schema: schema)
puts "   Dataset opened. Current count: #{dataset2.count}"
puts "   ✓ Data persisted from previous session!"

# Third call - still idempotent
puts "\n3. Third call - still works..."
dataset3 = Lancelot::Dataset.open_or_create(dataset_path, schema: schema)
dataset3.add_documents([
  { text: "more", embedding: Array.new(768) { rand } }
])
puts "   Added 1 more document. New count: #{dataset3.count}"

# Demonstrate the OLD way that would fail
puts "\n4. Compare with non-idempotent create (would fail)..."
begin
  # This will fail because dataset already exists
  failing_dataset = Lancelot::Dataset.create(dataset_path, schema: schema)
  puts "   ✗ This shouldn't happen!"
rescue => e
  puts "   ✓ Dataset.create correctly failed: #{e.class}"
  puts "   Message: #{e.message[0..50]}..."
end

# Clean up
FileUtils.rm_rf(dataset_path)

puts "\n" + "="*60
puts "Summary: Use open_or_create for idempotent operations!"
puts "="*60
puts "\nInstead of:"
puts '  dataset = Lancelot::Dataset.create("words", schema: {...})'
puts "\nUse:"
puts '  dataset = Lancelot::Dataset.open_or_create("words", schema: {...})'
puts "\nThis way your code works whether the dataset exists or not!"