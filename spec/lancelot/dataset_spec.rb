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

    describe "#create_index" do
      it "creates a vector index" do
        expect { dataset.create_index(column: "vector") }.not_to raise_error
      end
    end

    describe "#search" do
      before do
        dataset.create_index(column: "vector")
      end

      it "finds nearest neighbors" do
        query_vector = [0.15, 0.25, 0.35]
        results = dataset.search(query_vector, limit: 2)
        
        expect(results).to be_an(Array)
        expect(results.length).to eq(2)
        # The first two documents should be the closest
        texts = results.map { |doc| doc[:text] }
        expect(texts).to include("Ruby programming", "Python coding")
      end

      it "respects the limit parameter" do
        query_vector = [0.5, 0.5, 0.5]
        results = dataset.search(query_vector, limit: 1)
        
        expect(results.length).to eq(1)
      end
    end

    describe "#nearest_neighbors" do
      before do
        dataset.create_index(column: "vector")
      end

      it "is an alias for search" do
        query_vector = [0.1, 0.2, 0.3]
        results = dataset.nearest_neighbors(query_vector, k: 2)
        
        expect(results).to be_an(Array)
        expect(results.length).to eq(2)
      end
    end
  end
end