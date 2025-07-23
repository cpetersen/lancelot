# frozen_string_literal: true

require "bundler/gem_tasks"
require "rspec/core/rake_task"

RSpec::Core::RakeTask.new(:spec)

require "standard/rake"

require "rake/extensiontask"

task build: :compile

GEMSPEC = Gem::Specification.load("lancelot.gemspec")

Rake::ExtensionTask.new("lancelot", GEMSPEC) do |ext|
  ext.lib_dir = "lib/lancelot"
end

task default: %i[clobber compile spec standard]
