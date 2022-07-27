class Jets::Commands::AwsLocal::SourceMapping::Sqs
  attr_reader :arn, :klass, :meth

  def self.sqs_client
    @@sqs_client ||= Aws::SQS::Client.new
  end

  def initialize(klass:, meth:, arn:)
    @klass = klass
    @meth = meth
    @arn = arn
  end

  def run
    poller.poll(max_number_of_messages: 10) do |messages|
      records = messages.as_json
      @klass.process({ 'Records' => records }, {}, @meth)
    end
  end

  def poller
    @poller ||= Aws::SQS::QueuePoller.new(queue_url)
  end

  def queue_url
    @queue_url ||= self
                    .class
                    .sqs_client
                    .get_queue_url(queue_name: queue_name)
                    .queue_url
                    .gsub('localstack', 'localhost')
  end

  def queue_name
    @arn.split(':').last
  end
end