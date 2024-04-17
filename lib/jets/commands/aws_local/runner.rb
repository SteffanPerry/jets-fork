class Jets::Commands::AwsLocal::Runner
  attr_accessor :arn, :klass, :meth, :raw

  def self.run(mapping)
    new(mapping).run
  end

  def initialize(arn:, klass:, method:, raw:)
    @arn   = arn
    @klass = klass
    @meth  = method
    @raw   = raw
  end

  def run
    puts "mapping event source #{mapping_type} to #{klass}##{meth}"
    event_source
      .new(klass: klass, meth: meth, arn: arn)
      .run
  rescue => e
    puts e
    sleep 1
    retry
  end

  def event_source
    case mapping_type
    when 'sqs'
      ::Jets::Commands::AwsLocal::SourceMapping::Sqs
    when 'dynamodb'
      ::Jets::Commands::AwsLocal::SourceMapping::Dynamodb
    end
  end

  def mapping_type
    @mapping_type ||= arn.split(':').try(:[], 2)
  end
end