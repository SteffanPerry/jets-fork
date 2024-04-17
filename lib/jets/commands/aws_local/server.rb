class Jets::Commands::AwsLocal::Server
  attr_reader :mappings

  def self.run
    new.run
  end

  def initialize
    @mappings = []
  end

  def run
    fetch_mappings

    mappings
      .map { |mapping| invoke_runner(mapping) }
      .map(&:join)
  end

  def invoke_runner(mapping)
    Thread.new do
      puts "Starting mapping job: #{mapping}"
      ::Jets::Commands::AwsLocal::Runner.run(mapping)
    rescue
    end
  end

  def fetch_mappings
    jobs.each do |klass|
      event_sources = klass_mappings(klass)
      event_sources.each do |event_source|
        @mappings << event_source.merge(klass: klass)
      end
    end
  end

  def jobs
    @jobs ||= ::ObjectSpace
                .each_object(Class)
                .select { |klass| klass < ::ApplicationJob }
  end

  def klass_mappings(klass)
    [].tap do |arr|
      klass.send(:all_tasks).to_a.each do |key, val|
        val.associated_resources.each do |ar|
          ar.definition.each do |definition|
            next unless definition.dig("{namespace}EventSourceMapping", :type) == "AWS::Lambda::EventSourceMapping"

            arr << { method: key, arn: definition.dig("{namespace}EventSourceMapping", :properties, :event_source_arn), raw: definition }
          end
        end
      end
    end
  end
end