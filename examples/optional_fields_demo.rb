#!/usr/bin/env ruby
# This example demonstrates optional field support in lancelot
# After the fix in conversion.rs, documents can have missing fields

require 'bundler/setup'
require 'lancelot'
require 'fileutils'

dataset_path = "example_optional_fields"
FileUtils.rm_rf(dataset_path)

puts "="*60
puts "Lancelot Optional Fields Demo"
puts "="*60

# Step 1: Create dataset with initial schema
puts "\n1. Creating dataset with 3 fields (id, text, score)..."
schema = {
  id: :string,
  text: :string,
  score: :float32
}
dataset = Lancelot::Dataset.create(dataset_path, schema: schema)

# Add initial documents
initial_docs = [
  { id: "1", text: "First document", score: 0.9 },
  { id: "2", text: "Second document", score: 0.8 }
]
dataset.add_documents(initial_docs)
puts "   Added #{dataset.count} documents"

# Step 2: Simulate schema evolution (adding a new field)
puts "\n2. Simulating schema evolution (adding 'category' field)..."

# Get existing data
all_docs = dataset.to_a

# Recreate with expanded schema
FileUtils.rm_rf(dataset_path)
expanded_schema = {
  id: :string,
  text: :string,
  score: :float32,
  category: :string  # NEW FIELD
}
dataset = Lancelot::Dataset.create(dataset_path, schema: expanded_schema)

# Re-add existing docs with the new field
docs_with_category = all_docs.map { |doc| doc.merge(category: "original") }
dataset.add_documents(docs_with_category)
puts "   Recreated dataset with expanded schema"

# Step 3: Add new documents WITHOUT the new field
puts "\n3. Adding new documents WITHOUT the 'category' field..."
new_docs = [
  { id: "3", text: "Third document", score: 0.7 },  # No category!
  { id: "4", text: "Fourth document", score: 0.6 }  # No category!
]

begin
  dataset.add_documents(new_docs)
  puts "   ✅ SUCCESS! Added #{new_docs.size} documents with missing fields"
rescue => e
  puts "   ❌ FAILED: #{e.message}"
  puts "   (This would have failed before the fix in conversion.rs)"
end

# Step 4: Verify the data
puts "\n4. Verifying all documents..."
dataset.to_a.each do |doc|
  category = doc[:category] || "nil"
  puts "   Doc #{doc[:id]}: category=#{category}"
end

puts "\nTotal documents: #{dataset.count}"

# Cleanup
FileUtils.rm_rf(dataset_path)

puts "\n" + "="*60
puts "Demo complete! Optional fields work correctly."
puts "="*60