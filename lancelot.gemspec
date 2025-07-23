# frozen_string_literal: true

require_relative "lib/lancelot/version"

Gem::Specification.new do |spec|
  spec.name = "lancelot"
  spec.version = Lancelot::VERSION
  spec.authors = ["Chris Petersen"]
  spec.email = ["chris@petersen.io"]

  spec.summary = "Ruby bindings for Lance - a modern columnar data format for ML"
  spec.description = "Lancelot provides a Ruby-native interface to Lance, enabling efficient storage and search of multimodal data including text, vectors, and more."
  spec.homepage = "https://github.com/assaydepot/lancelot"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.1.0"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/assaydepot/lancelot"
  spec.metadata["changelog_uri"] = "https://github.com/assaydepot/lancelot/blob/main/CHANGELOG.md"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  gemspec = File.basename(__FILE__)
  spec.files = IO.popen(%w[git ls-files -z], chdir: __dir__, err: IO::NULL) do |ls|
    ls.readlines("\x0", chomp: true).reject do |f|
      (f == gemspec) ||
        f.start_with?(*%w[bin/ test/ spec/ features/ .git appveyor Gemfile])
    end
  end
  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]
  spec.extensions = ["ext/lancelot/extconf.rb"]

  # Runtime dependencies
  spec.add_dependency "rb_sys", "~> 0.9"

  # Development dependencies
  spec.add_development_dependency "rake", "~> 13.0"
  spec.add_development_dependency "rake-compiler", "~> 1.2"
  spec.add_development_dependency "rspec", "~> 3.0"
  spec.add_development_dependency "standard", "~> 1.3"

  # For more information and examples about making a new gem, check out our
  # guide at: https://bundler.io/guides/creating_gem.html
end
