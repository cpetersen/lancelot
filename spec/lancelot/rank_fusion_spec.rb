# frozen_string_literal: true

require "lancelot/rank_fusion"

RSpec.describe Lancelot::RankFusion do
  describe ".reciprocal_rank_fusion" do
    context "with valid inputs" do
      it "returns empty array for empty input" do
        expect(described_class.reciprocal_rank_fusion([])).to eq([])
        expect(described_class.reciprocal_rank_fusion(nil)).to eq([])
      end

      it "returns empty array when all result lists are empty" do
        expect(described_class.reciprocal_rank_fusion([[], [], []])).to eq([])
      end

      it "returns single list results when only one list provided" do
        results = [
          {id: 1, text: "Document 1"},
          {id: 2, text: "Document 2"}
        ]

        fused = described_class.reciprocal_rank_fusion([results])

        expect(fused.size).to eq(2)
        expect(fused[0]).to include(id: 1, text: "Document 1", rrf_score: 1.0 / 61)
        expect(fused[1]).to include(id: 2, text: "Document 2", rrf_score: 1.0 / 62)
      end

      it "combines results from multiple lists using RRF algorithm" do
        list1 = [
          {id: 1, text: "Doc A"},
          {id: 2, text: "Doc B"},
          {id: 3, text: "Doc C"}
        ]

        list2 = [
          {id: 2, text: "Doc B"},
          {id: 1, text: "Doc A"},
          {id: 4, text: "Doc D"}
        ]

        fused = described_class.reciprocal_rank_fusion([list1, list2])

        expect(fused.size).to eq(4)

        # Doc B appears at rank 2 in list1 and rank 1 in list2
        # Score = 1/(60+2) + 1/(60+1) = 1/62 + 1/61 ≈ 0.0325
        doc_b = fused.find { |d| d[:id] == 2 }
        expect(doc_b[:rrf_score]).to be_within(0.0001).of(1.0 / 62 + 1.0 / 61)

        # Doc A appears at rank 1 in list1 and rank 2 in list2
        # Score = 1/(60+1) + 1/(60+2) = 1/61 + 1/62 ≈ 0.0325
        doc_a = fused.find { |d| d[:id] == 1 }
        expect(doc_a[:rrf_score]).to be_within(0.0001).of(1.0 / 61 + 1.0 / 62)

        # Docs should be sorted by RRF score
        expect([1, 2]).to include(fused[0][:id]) # A and B have same score
        expect([1, 2]).to include(fused[1][:id])
      end

      it "handles documents that appear in only some lists" do
        list1 = [
          {id: 1, text: "Doc A"},
          {id: 2, text: "Doc B"}
        ]

        list2 = [
          {id: 3, text: "Doc C"},
          {id: 4, text: "Doc D"}
        ]

        fused = described_class.reciprocal_rank_fusion([list1, list2])

        expect(fused.size).to eq(4)

        # Each doc appears in only one list, so score = 1/(60+rank)
        doc_a = fused.find { |d| d[:id] == 1 }
        expect(doc_a[:rrf_score]).to be_within(0.0001).of(1.0 / 61)
      end

      it "uses custom k value when provided" do
        results = [
          [{id: 1, text: "Doc 1"}]
        ]

        fused = described_class.reciprocal_rank_fusion(results, k: 100)

        expect(fused[0][:rrf_score]).to be_within(0.0001).of(1.0 / 101)
      end

      it "ignores metadata fields when matching documents" do
        list1 = [
          {id: 1, text: "Doc A", _distance: 0.5}
        ]

        list2 = [
          {id: 1, text: "Doc A", _score: 0.8}
        ]

        fused = described_class.reciprocal_rank_fusion([list1, list2])

        expect(fused.size).to eq(1)
        # Document appears at rank 1 in both lists
        expect(fused[0][:rrf_score]).to be_within(0.0001).of(2.0 / 61)
      end

      it "preserves all document fields in output" do
        doc = {id: 1, text: "Document", metadata: {author: "Test"}, score: 0.9}
        fused = described_class.reciprocal_rank_fusion([[doc]])

        expect(fused[0]).to include(
          id: 1,
          text: "Document",
          metadata: {author: "Test"},
          score: 0.9,
          rrf_score: 1.0 / 61
        )
      end
    end

    context "with invalid inputs" do
      it "raises ArgumentError for non-array result lists" do
        expect {
          described_class.reciprocal_rank_fusion("not an array")
        }.to raise_error(ArgumentError, /must be an Array/)
      end

      it "raises ArgumentError for non-hash documents" do
        expect {
          described_class.reciprocal_rank_fusion([["not a hash"]])
        }.to raise_error(ArgumentError, /must be a Hash/)
      end

      it "raises ArgumentError for mixed valid and invalid documents" do
        expect {
          described_class.reciprocal_rank_fusion([[{id: 1}, "not a hash"]])
        }.to raise_error(ArgumentError, /must be a Hash/)
      end
    end

    context "edge cases" do
      it "handles single document across multiple lists" do
        doc = {id: 1, text: "Same doc"}
        fused = described_class.reciprocal_rank_fusion([[doc], [doc], [doc]])

        expect(fused.size).to eq(1)
        # Appears at rank 1 in all 3 lists: 3 * (1/61)
        expect(fused[0][:rrf_score]).to be_within(0.0001).of(3.0 / 61)
      end

      it "correctly ranks documents with different positions" do
        list1 = (1..5).map { |i| {id: i, text: "Doc #{i}"} }
        list2 = list1.reverse

        fused = described_class.reciprocal_rank_fusion([list1, list2])

        # Middle document (id: 3) should have highest score
        # as it appears at rank 3 in both lists
        scores = fused.map { |d| [d[:id], d[:rrf_score]] }.to_h

        # Doc 3: rank 3 in both lists = 2/(60+3) ≈ 0.0317
        expect(scores[3]).to be_within(0.0001).of(2.0 / 63)

        # Doc 1: rank 1 in list1, rank 5 in list2
        expect(scores[1]).to be_within(0.0001).of(1.0 / 61 + 1.0 / 65)

        # Doc 5: rank 5 in list1, rank 1 in list2
        expect(scores[5]).to be_within(0.0001).of(1.0 / 65 + 1.0 / 61)
      end

      it "maintains deterministic ordering for equal scores" do
        doc1 = {id: 1, text: "A"}
        doc2 = {id: 2, text: "B"}

        # Both docs at same rank in their respective lists
        result1 = described_class.reciprocal_rank_fusion([[doc1], [doc2]])
        result2 = described_class.reciprocal_rank_fusion([[doc1], [doc2]])

        expect(result1.map { |d| d[:id] }).to eq(result2.map { |d| d[:id] })
      end
    end
  end
end

