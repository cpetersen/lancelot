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
end