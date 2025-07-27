# frozen_string_literal: true

require_relative "lancelot/version"
require_relative "lancelot/lancelot"
require_relative "lancelot/dataset"
require_relative "lancelot/rank_fusion"

module Lancelot
  class Error < StandardError; end
end
