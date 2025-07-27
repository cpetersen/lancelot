# frozen_string_literal: true

module Lancelot
  module RankFusion
    class << self
      def reciprocal_rank_fusion(result_lists, k: 60)
        return [] if result_lists.nil? || result_lists.empty?

        # Validate inputs
        result_lists = Array(result_lists)
        validate_result_lists(result_lists)

        return [] if result_lists.all?(&:empty?)

        # Build document to rank mapping for each result list
        doc_ranks = build_document_ranks(result_lists)

        # Calculate RRF scores
        rrf_scores = calculate_rrf_scores(doc_ranks, result_lists.size, k)

        # Sort by RRF score descending and return results
        rrf_scores.sort_by { |_, score| -score }.map do |doc, score|
          doc.merge(rrf_score: score)
        end
      end

      private

      def validate_result_lists(result_lists)
        result_lists.each_with_index do |list, i|
          unless list.is_a?(Array)
            raise ArgumentError, "Result list at index #{i} must be an Array, got #{list.class}"
          end

          list.each_with_index do |doc, j|
            unless doc.is_a?(Hash)
              raise ArgumentError, "Document at position #{j} in result list #{i} must be a Hash, got #{doc.class}"
            end
          end
        end
      end

      def build_document_ranks(result_lists)
        doc_ranks = {}

        result_lists.each_with_index do |list, list_idx|
          list.each_with_index do |doc, rank|
            # Use the document content as the key (excluding metadata like distance/score)
            doc_key = normalize_document(doc)
            doc_ranks[doc_key] ||= {document: doc, ranks: {}}
            doc_ranks[doc_key][:ranks][list_idx] = rank + 1  # 1-based ranking
          end
        end

        doc_ranks
      end

      def calculate_rrf_scores(doc_ranks, num_lists, k)
        doc_ranks.map do |doc_key, data|
          score = 0.0

          num_lists.times do |list_idx|
            rank = data[:ranks][list_idx]
            if rank
              score += 1.0 / (k + rank)
            else
              # Document doesn't appear in this list, treat as infinite rank
              # RRF score contribution is effectively 0
            end
          end

          [data[:document], score]
        end
      end

      def normalize_document(doc)
        # Create a normalized version for comparison, excluding metadata fields
        # that might differ between search types (like _distance, _score, etc.)
        doc.reject { |k, _| k.to_s.start_with?("_") || k == :rrf_score }
      end
    end
  end
end

