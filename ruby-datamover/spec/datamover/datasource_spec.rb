# frozen_string_literal: true
require 'spec_helper'

describe TC::Datasource do
  describe 'require' do
    it 'can be required' do
      expect { load './lib/datamover/datasource.rb' }.to_not raise_error
    end

    context 'when missing path to datasource.yml file' do
      it 'can still be required' do
        allow(ENV).to receive(:[]).and_call_original
        allow(ENV).to receive(:[]).with('datamover_datasource_path').and_return(nil)

        expect { load './lib/datamover/datasource.rb' }.to_not raise_error
      end
    end
  end
end
