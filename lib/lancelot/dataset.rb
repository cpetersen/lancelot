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

    def text_search(query, column: nil, columns: nil, limit: 10)
      unless query.is_a?(String)
        raise ArgumentError, "Query must be a string"
      end
      
      if column && columns
        raise ArgumentError, "Cannot specify both column and columns"
      elsif columns
        # Multi-column search
        columns = Array(columns).map(&:to_s)
        _rust_multi_column_text_search(columns, query, limit)
      else
        # Single column search (default to "text" if not specified)
        column ||= "text"
        _rust_text_search(column.to_s, query, limit)
      end
    end

    def hybrid_search(query, vector_column: "vector", text_column: nil, text_columns: nil, 
                      vector: nil, limit: 10, rrf_k: 60)
      require 'lancelot/rank_fusion'
      
      result_lists = []
      
      # Perform vector search if vector is provided
      if vector
        unless vector.is_a?(Array)
          raise ArgumentError, "Vector must be an array of numbers"
        end
        
        vector_results = vector_search(vector, column: vector_column, limit: limit * 2)
        result_lists << vector_results if vector_results.any?
      end
      
      # Perform text search if query is provided
      if query && !query.empty?
        text_results = text_search(query, column: text_column, columns: text_columns, limit: limit * 2)
        result_lists << text_results if text_results.any?
      end
      
      # Return empty array if no searches were performed
      return [] if result_lists.empty?
      
      # Return single result list if only one search was performed
      return result_lists.first[0...limit] if result_lists.size == 1
      
      # Perform RRF fusion and limit results
      Lancelot::RankFusion.reciprocal_rank_fusion(result_lists, k: rrf_k)[0...limit]
    end

    def where(filter_expression, limit: nil)
      filter_scan(filter_expression.to_s, limit)
    end

    def to_s
      "#<Lancelot::Dataset path=\"#{path}\" count=#{count}>"
    end
    alias inspect to_s

    def ==(other)
      other.is_a?(Dataset) && other.path == path
    end
    alias eql? ==

    def hash
      path.hash
    end

    private

    def normalize_document(doc)
      doc.transform_keys(&:to_sym)
    end
  end
end