# frozen_string_literal: true
require 'spec_helper'

describe ::Datamover do
  let(:datamover) { ::Datamover }

  it { expect(datamover::VERSION).to_not be nil }
end
