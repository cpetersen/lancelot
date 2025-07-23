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

    private

    def normalize_document(doc)
      doc.transform_keys(&:to_sym)
    end
  end
end