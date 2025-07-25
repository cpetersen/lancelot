# frozen_string_literal: true

module Lancelot
  class Dataset
    class << self
      def create(path, schema:)
        dataset = new(path)
        dataset.create(normalize_schema(schema))
        dataset
      end

      def open(path)
        dataset = new(path)
        dataset.open
        dataset
      end

      private

      def normalize_schema(schema)
        schema.transform_values do |type|
          case type
          when Hash
            type
          when :string, "string"
            "string"
          when :float, :float32, "float", "float32"
            "float32"
          when :float64, "float64"
            "float64"
          when :int, :int32, "int", "int32"
            "int32"
          when :int64, "int64"
            "int64"
          when :bool, :boolean, "bool", "boolean"
            "boolean"
          else
            raise ArgumentError, "Unknown type: #{type}"
          end
        end
      end
    end

    def add_documents(documents)
      add_data(documents.map { |doc| normalize_document(doc) })
    end

    def <<(document)
      add_documents([document])
      self
    end

    def size
      count_rows
    end

    alias_method :count, :size
    alias_method :length, :size

    def all
      scan_all
    end

    def first(n = nil)
      if n.nil?
        scan_limit(1).first
      else
        scan_limit(n)
      end
    end

    def each(&block)
      return enum_for(:each) unless block_given?
      scan_all.each(&block)
    end

    include Enumerable

    def vector_search(query_vector, column: "vector", limit: 10)
      unless query_vector.is_a?(Array)
        raise ArgumentError, "Query vector must be an array of numbers"
      end
      
      _rust_vector_search(column.to_s, query_vector, limit)
    end

    def nearest_neighbors(vector, k: 10, column: "vector")
      vector_search(vector, column: column, limit: k)
    end

    def text_search(query, column: "text", limit: 10)
      unless query.is_a?(String)
        raise ArgumentError, "Query must be a string"
      end
      
      # Call the underlying Rust method with parameters in correct order
      _rust_text_search(column.to_s, query, limit)
    end

    def where(filter_expression, limit: nil)
      filter_scan(filter_expression.to_s, limit)
    end

    private

    def normalize_document(doc)
      doc.transform_keys(&:to_sym)
    end
  end
end