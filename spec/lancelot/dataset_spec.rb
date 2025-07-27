# frozen_string_literal: true

require "tempfile"
require "fileutils"

RSpec.describe Lancelot::Dataset do
  let(:temp_dir) { Dir.mktmpdir }
  let(:dataset_path) { File.join(temp_dir, "test_dataset") }

  after do
    FileUtils.rm_rf(temp_dir)
  end

  describe ".create" do
    it "creates a new dataset with a schema" do
      schema = {
        text: :string,
        score: :float32,
        embedding: { type: "vector", dimension: 3 }
      }

      dataset = Lancelot::Dataset.create(dataset_path, schema: schema)
      expect(dataset).to be_a(Lancelot::Dataset)
      expect(dataset.path).to eq(dataset_path)
    end

    it "normalizes schema types" do
      schema = {
        text: "string",
        score: :float
      }

      dataset = Lancelot::Dataset.create(dataset_path, schema: schema)
      expect(dataset.schema).to eq({
        text: "string",
        score: "float32"
      })
    end
  end

  describe ".open" do
    it "opens an existing dataset" do
      schema = { text: :string, score: :float32 }
      Lancelot::Dataset.create(dataset_path, schema: schema)

      dataset = Lancelot::Dataset.open(dataset_path)
      expect(dataset).to be_a(Lancelot::Dataset)
      expect(dataset.schema).to eq({ text: "string", score: "float32" })
    end
  end

  describe "#add_documents" do
    let(:dataset) do
      schema = {
        text: :string,
        score: :float32,
        embedding: { type: "vector", dimension: 3 }
      }
      Lancelot::Dataset.create(dataset_path, schema: schema)
    end

    it "adds documents to the dataset" do
      documents = [
        { text: "Hello world", score: 0.9, embedding: [0.1, 0.2, 0.3] },
        { text: "Ruby is great", score: 0.8, embedding: [0.4, 0.5, 0.6] }
      ]

      dataset.add_documents(documents)
      expect(dataset.count).to eq(2)
    end

    it "accepts string keys" do
      documents = [
        { "text" => "Hello", "score" => 0.5, "embedding" => [1.0, 2.0, 3.0] }
      ]

      expect { dataset.add_documents(documents) }.not_to raise_error
      expect(dataset.count).to eq(1)
    end
  end

  describe "#<<" do
    let(:dataset) do
      schema = { text: :string, score: :float32 }
      Lancelot::Dataset.create(dataset_path, schema: schema)
    end

    it "adds a single document" do
      dataset << { text: "Hello", score: 0.7 }
      expect(dataset.count).to eq(1)
    end

    it "returns self for chaining" do
      result = dataset << { text: "First", score: 0.1 } << { text: "Second", score: 0.2 }
      expect(result).to eq(dataset)
      expect(dataset.count).to eq(2)
    end
  end

  describe "#size, #count, #length" do
    let(:dataset) do
      schema = { text: :string, score: :float32 }
      Lancelot::Dataset.create(dataset_path, schema: schema)
    end

    it "returns the number of rows" do
      expect(dataset.size).to eq(0)
      
      dataset.add_documents([
        { text: "Hello", score: 0.5 }, 
        { text: "World", score: 0.8 }
      ])
      
      expect(dataset.size).to eq(2)
      expect(dataset.count).to eq(2)
      expect(dataset.length).to eq(2)
    end
  end

  describe "document retrieval" do
    let(:dataset) do
      schema = { text: :string, score: :float32 }
      Lancelot::Dataset.create(dataset_path, schema: schema)
    end

    before do
      dataset.add_documents([
        { text: "Ruby is great", score: 0.95 },
        { text: "Python is cool", score: 0.75 },
        { text: "JavaScript is everywhere", score: 0.85 }
      ])
    end

    describe "#all" do
      it "returns all documents" do
        docs = dataset.all
        expect(docs).to be_an(Array)
        expect(docs.length).to eq(3)
        expect(docs.first[:text]).to eq("Ruby is great")
      end
    end

    describe "#first" do
      it "returns the first document when called without argument" do
        doc = dataset.first
        expect(doc).to be_a(Hash)
        expect(doc[:text]).to eq("Ruby is great")
      end

      it "returns the first n documents when called with argument" do
        docs = dataset.first(2)
        expect(docs).to be_an(Array)
        expect(docs.length).to eq(2)
        expect(docs[0][:text]).to eq("Ruby is great")
        expect(docs[1][:text]).to eq("Python is cool")
      end
    end

    describe "#each" do
      it "yields each document" do
        texts = []
        dataset.each { |doc| texts << doc[:text] }
        expect(texts).to eq(["Ruby is great", "Python is cool", "JavaScript is everywhere"])
      end

      it "returns an enumerator when no block given" do
        enum = dataset.each
        expect(enum).to be_an(Enumerator)
        expect(enum.to_a.length).to eq(3)
      end
    end

    describe "Enumerable methods" do
      it "supports map" do
        texts = dataset.map { |doc| doc[:text] }
        expect(texts).to eq(["Ruby is great", "Python is cool", "JavaScript is everywhere"])
      end

      it "supports select" do
        high_score_docs = dataset.select { |doc| doc[:score] && doc[:score] >= 0.9 }
        expect(high_score_docs.length).to eq(1)
        expect(high_score_docs.first[:text]).to eq("Ruby is great")
      end
    end
  end

  describe "vector search" do
    let(:dataset) do
      schema = { 
        text: :string, 
        score: :float32,
        vector: { type: "vector", dimension: 3 }
      }
      Lancelot::Dataset.create(dataset_path, schema: schema)
    end

    before do
      dataset.add_documents([
        { text: "Ruby programming", score: 0.9, vector: [0.1, 0.2, 0.3] },
        { text: "Python coding", score: 0.85, vector: [0.2, 0.3, 0.4] },
        { text: "JavaScript development", score: 0.8, vector: [0.8, 0.9, 0.7] }
      ])
    end

    describe "#create_vector_index" do
      it "creates a vector index" do
        expect { dataset.create_vector_index("vector") }.not_to raise_error
      end
    end

    describe "#vector_search" do
      before do
        dataset.create_vector_index("vector")
      end

      it "finds nearest neighbors" do
        query_vector = [0.15, 0.25, 0.35]
        results = dataset.vector_search(query_vector, column: "vector", limit: 2)
        
        expect(results).to be_an(Array)
        expect(results.length).to eq(2)
        # The first two documents should be the closest
        texts = results.map { |doc| doc[:text] }
        expect(texts).to include("Ruby programming", "Python coding")
      end

      it "respects the limit parameter" do
        query_vector = [0.5, 0.5, 0.5]
        results = dataset.vector_search(query_vector, column: "vector", limit: 1)
        
        expect(results.length).to eq(1)
      end

      it "raises error for non-array query" do
        expect {
          dataset.vector_search("not an array", column: "vector")
        }.to raise_error(ArgumentError, /must be an array/)
      end
    end

    describe "#nearest_neighbors" do
      before do
        dataset.create_vector_index("vector")
      end

      it "calls vector_search with k parameter" do
        query_vector = [0.1, 0.2, 0.3]
        results = dataset.nearest_neighbors(query_vector, k: 2, column: "vector")
        
        expect(results).to be_an(Array)
        expect(results.length).to eq(2)
      end
    end
  end

  describe "text search" do
    let(:dataset) do
      schema = { 
        title: :string,
        content: :string,
        category: :string,
        year: :int64
      }
      Lancelot::Dataset.create(dataset_path, schema: schema)
    end

    before do
      dataset.add_documents([
        { title: "Ruby on Rails", content: "Web framework for Ruby", category: "web", year: 2023 },
        { title: "Django Python", content: "Web framework for Python", category: "web", year: 2024 },
        { title: "Ruby Gems", content: "Package manager for Ruby", category: "tools", year: 2023 },
        { title: "Python Packages", content: "PyPI is the Python package index", category: "tools", year: 2024 }
      ])
    end

    describe "#create_text_index" do
      it "creates a text index on a column" do
        expect { dataset.create_text_index("title") }.not_to raise_error
        expect { dataset.create_text_index("content") }.not_to raise_error
      end
    end

    describe "#text_search" do
      context "with text indices" do
        before do
          dataset.create_text_index("title")
          dataset.create_text_index("content")
          dataset.create_text_index("category")
        end

        it "searches a single column" do
          results = dataset.text_search("ruby", column: "title")
          expect(results).to be_an(Array)
          expect(results.length).to eq(2)
          titles = results.map { |doc| doc[:title] }
          expect(titles).to include("Ruby on Rails", "Ruby Gems")
        end

        it "searches with default column" do
          # Default is "text" column which doesn't exist
          # This will raise an error because the column doesn't exist
          expect {
            dataset.text_search("ruby")
          }.to raise_error(RuntimeError, /Column text not found/)
        end

        it "searches multiple columns" do
          results = dataset.text_search("framework", columns: ["title", "content"])
          expect(results.length).to be >= 2
        end

        it "is case insensitive" do
          results = dataset.text_search("RUBY", column: "title")
          expect(results.length).to eq(2)
        end

        it "handles multi-word queries" do
          results = dataset.text_search("package manager", column: "content")
          expect(results.length).to be >= 1
          expect(results.first[:title]).to eq("Ruby Gems")
        end

        it "raises error for non-string query" do
          expect {
            dataset.text_search(123, column: "title")
          }.to raise_error(ArgumentError, /must be a string/)
        end

        it "raises error when both column and columns specified" do
          expect {
            dataset.text_search("ruby", column: "title", columns: ["content"])
          }.to raise_error(ArgumentError, /Cannot specify both/)
        end
      end
    end

    describe "#where" do
      it "filters with simple conditions" do
        results = dataset.where("year = 2023")
        expect(results.length).to eq(2)
        expect(results.map { |doc| doc[:title] }).to include("Ruby on Rails", "Ruby Gems")
      end

      it "filters with compound conditions" do
        results = dataset.where("category = 'web' AND year = 2024")
        expect(results.length).to eq(1)
        expect(results.first[:title]).to eq("Django Python")
      end

      it "filters with LIKE patterns" do
        results = dataset.where("title LIKE '%Python%'")
        expect(results.length).to eq(2)
      end

      it "supports limit parameter" do
        results = dataset.where("category = 'web'", limit: 1)
        expect(results.length).to eq(1)
      end

      it "handles OR conditions" do
        results = dataset.where("title LIKE '%Rails%' OR title LIKE '%Django%'")
        expect(results.length).to eq(2)
      end
    end
  end

  describe "Ruby object methods" do
    let(:dataset) do
      schema = { text: :string, score: :float32 }
      ds = Lancelot::Dataset.create(dataset_path, schema: schema)
      # Add at least one document so the dataset is properly initialized
      ds.add_documents([{ text: "test", score: 0.5 }])
      ds
    end

    describe "#to_s and #inspect" do
      it "returns a string representation with path and count" do
        dataset.add_documents([
          { text: "First", score: 0.5 },
          { text: "Second", score: 0.8 }
        ])
        
        expected = "#<Lancelot::Dataset path=\"#{dataset_path}\" count=3>"
        expect(dataset.to_s).to eq(expected)
        expect(dataset.inspect).to eq(expected)
      end

      it "shows count for dataset with initial document" do
        expected = "#<Lancelot::Dataset path=\"#{dataset_path}\" count=1>"
        expect(dataset.to_s).to eq(expected)
      end
    end

    describe "#== and #eql?" do
      it "returns true for datasets with the same path" do
        # Ensure dataset is fully written by calling count
        expect(dataset.count).to eq(1)
        
        dataset2 = Lancelot::Dataset.open(dataset_path)
        
        expect(dataset == dataset2).to be true
        expect(dataset.eql?(dataset2)).to be true
      end

      it "returns false for datasets with different paths" do
        other_path = File.join(temp_dir, "other_dataset")
        other_dataset = Lancelot::Dataset.create(other_path, schema: { text: :string })
        
        expect(dataset == other_dataset).to be false
        expect(dataset.eql?(other_dataset)).to be false
      end

      it "returns false when comparing with non-dataset objects" do
        expect(dataset == "not a dataset").to be false
        expect(dataset == nil).to be false
        expect(dataset == 123).to be false
      end
    end

    describe "#hash" do
      it "returns the same hash for datasets with the same path" do
        # Ensure dataset is fully written
        expect(dataset.count).to eq(1)
        
        dataset2 = Lancelot::Dataset.open(dataset_path)
        
        expect(dataset.hash).to eq(dataset2.hash)
      end

      it "returns different hashes for datasets with different paths" do
        other_path = File.join(temp_dir, "other_dataset")
        other_dataset = Lancelot::Dataset.create(other_path, schema: { text: :string })
        
        expect(dataset.hash).not_to eq(other_dataset.hash)
      end

      it "can be used as a hash key" do
        # Ensure dataset is fully written
        expect(dataset.count).to eq(1)
        
        dataset2 = Lancelot::Dataset.open(dataset_path)
        
        hash = {}
        hash[dataset] = "value1"
        hash[dataset2] = "value2"
        
        # Should overwrite since they're the same dataset
        expect(hash.size).to eq(1)
        expect(hash[dataset]).to eq("value2")
        expect(hash[dataset2]).to eq("value2")
      end

      it "can be used in a Set" do
        require 'set'
        
        # Ensure dataset is fully written
        expect(dataset.count).to eq(1)
        
        dataset2 = Lancelot::Dataset.open(dataset_path)
        
        set = Set.new
        set.add(dataset)
        set.add(dataset2)
        
        # Should only have one element since they're the same dataset
        expect(set.size).to eq(1)
      end
    end

    describe "#path" do
      it "returns the dataset path" do
        expect(dataset.path).to eq(dataset_path)
      end
    end
  end

  describe "#hybrid_search" do
    let(:dataset) do
      schema = {
        title: :string,
        content: :string,
        embedding: { type: "vector", dimension: 3 }
      }
      Lancelot::Dataset.create(dataset_path, schema: schema)
    end

    before do
      documents = [
        { 
          title: "Ruby on Rails", 
          content: "A web framework for Ruby",
          embedding: [0.1, 0.2, 0.3]
        },
        { 
          title: "Python Django", 
          content: "A web framework for Python",
          embedding: [0.4, 0.5, 0.6]
        },
        { 
          title: "Ruby Gems", 
          content: "Package manager for Ruby",
          embedding: [0.2, 0.3, 0.4]
        },
        { 
          title: "JavaScript Express", 
          content: "A minimal web framework",
          embedding: [0.7, 0.8, 0.9]
        }
      ]
      
      dataset.add_documents(documents)
      dataset.create_vector_index("embedding")
      dataset.create_text_index("title")
      dataset.create_text_index("content")
    end

    it "combines vector and text search results" do
      query_vector = [0.15, 0.25, 0.35]
      results = dataset.hybrid_search(
        "Ruby",
        vector: query_vector,
        vector_column: "embedding",
        text_column: "title",
        limit: 3
      )
      
      expect(results).to be_an(Array)
      expect(results.length).to be <= 3
      
      # Results should have RRF scores
      results.each do |doc|
        expect(doc).to have_key(:rrf_score)
        expect(doc[:rrf_score]).to be_a(Float)
        expect(doc[:rrf_score]).to be > 0
      end
      
      # Should be sorted by RRF score descending
      scores = results.map { |doc| doc[:rrf_score] }
      expect(scores).to eq(scores.sort.reverse)
    end

    it "works with only vector search" do
      query_vector = [0.1, 0.2, 0.3]
      results = dataset.hybrid_search(
        nil,
        vector: query_vector,
        vector_column: "embedding",
        limit: 2
      )
      
      expect(results).to be_an(Array)
      expect(results.length).to be <= 2
      # Should not have RRF scores when only one search type
      expect(results.first).not_to have_key(:rrf_score)
    end

    it "works with only text search" do
      results = dataset.hybrid_search(
        "framework",
        text_column: "content",
        limit: 2
      )
      
      expect(results).to be_an(Array)
      expect(results.length).to be <= 2
      # Should not have RRF scores when only one search type
      expect(results.first).not_to have_key(:rrf_score)
    end

    it "supports multi-column text search" do
      query_vector = [0.2, 0.3, 0.4]
      results = dataset.hybrid_search(
        "Ruby",
        vector: query_vector,
        vector_column: "embedding",
        text_columns: ["title", "content"],
        limit: 3
      )
      
      expect(results).to be_an(Array)
      expect(results.length).to be <= 3
      results.each do |doc|
        expect(doc).to have_key(:rrf_score)
      end
    end

    it "returns empty array when no results match" do
      results = dataset.hybrid_search(
        "NonexistentTerm",
        text_column: "title",
        limit: 10
      )
      
      expect(results).to eq([])
    end

    it "returns empty array when neither query nor vector provided" do
      results = dataset.hybrid_search(nil, limit: 10)
      expect(results).to eq([])
      
      results = dataset.hybrid_search("", limit: 10)
      expect(results).to eq([])
    end

    it "respects custom RRF k parameter" do
      query_vector = [0.1, 0.2, 0.3]
      results = dataset.hybrid_search(
        "Ruby",
        vector: query_vector,
        vector_column: "embedding",
        text_column: "title",
        limit: 2,
        rrf_k: 100
      )
      
      expect(results).to be_an(Array)
      results.each do |doc|
        expect(doc).to have_key(:rrf_score)
      end
    end

    it "raises error for invalid vector" do
      expect {
        dataset.hybrid_search("Ruby", vector: "not an array", text_column: "title")
      }.to raise_error(ArgumentError, /Vector must be an array/)
    end

    it "handles documents appearing in only one result set" do
      # Use a query that will return different documents in each search
      query_vector = [0.9, 0.9, 0.9]  # Closer to JavaScript document
      results = dataset.hybrid_search(
        "Ruby",  # Will match Ruby documents
        vector: query_vector,
        vector_column: "embedding",
        text_column: "title",
        limit: 4
      )
      
      expect(results).to be_an(Array)
      # Should include documents from both searches
      titles = results.map { |doc| doc[:title] }
      expect(titles).to include("JavaScript Express") # From vector search
      expect(titles).to include("Ruby on Rails")      # From text search
      expect(titles).to include("Ruby Gems")          # From text search
    end

    it "properly deduplicates documents across result sets" do
      # Use similar vector to Ruby documents
      query_vector = [0.15, 0.25, 0.35]
      results = dataset.hybrid_search(
        "Ruby",
        vector: query_vector,
        vector_column: "embedding",
        text_column: "title",
        limit: 10
      )
      
      # Count occurrences of each document
      title_counts = results.group_by { |doc| doc[:title] }.transform_values(&:count)
      
      # Each document should appear only once
      title_counts.each do |_title, count|
        expect(count).to eq(1)
      end
    end
  end
end