# frozen_string_literal: true

RSpec.describe Lancelot do
  it "has a version number" do
    expect(Lancelot::VERSION).not_to be nil
  end

  it "responds to hello" do
    expect(Lancelot.hello).to eq("Hello from Lancelot with Lance!")
  end

  describe Lancelot::Dataset do
    it "can be initialized with a path" do
      dataset = Lancelot::Dataset.new("test/path")
      expect(dataset.path).to eq("test/path")
    end
  end
end
