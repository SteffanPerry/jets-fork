class Jets::Commands::AwsLocal::SourceMapping::Dynamodb
  attr_reader :arn, :klass, :meth, :shard_iterators

  def initialize(klass:, meth:, arn:)
    @klass = klass
    @meth = meth
    @arn = arn
    @shard_iterators = []
  end

  def run
    fetch_latest_arn
    fetch_iterators

    loop do
      @shard_iterators.map! do |shard_iterator|
        resp = client.get_records(shard_iterator: shard_iterator)
        process_response(resp)
        resp.next_shard_iterator || shard_iterator
      rescue Aws::DynamoDBStreams::Errors::ExpiredIteratorException
        
      end
      sleep 2
    end
  end

  def fetch_latest_arn
    table_name = arn.split('/')[1]
    resp = dynamodb.describe_table(table_name: table_name)
    @arn = resp.table.latest_stream_arn.gsub('us-east-1', 'ddblocal')
  end

  def fetch_iterators
    @shard_iterators = shards.map do |shard|
      fetch_iterator(shard)
    end
  end

  def fetch_iterator(shard)
    client
      .get_shard_iterator(stream_arn: @arn, shard_id: shard[:shard_id], shard_iterator_type: 'LATEST')
      .shard_iterator
  end

  def stream_arns
    @streams ||= client.list_streams.streams.map{ |stream| stream.stream_arn.gsub('us-east-1', 'ddblocal') }
  end

  def process_response(resp)
    raw_response = JSON.parse(resp.instance_variable_get(:@http_response).body.to_json)
    records = JSON.parse(raw_response.first)
    @klass.process(records, {}, :events) unless resp.records.size.zero?
  end

  def shards
    @shards ||= client.describe_stream(stream_arn: @arn).dig(:stream_description, :shards)
  end

  def shard_iterators
    @shard_iterators ||= shards.map do |shard|
      client
        .get_shard_iterator(stream_arn: @arn, shard_id: shard[:shard_id], shard_iterator_type: 'LATEST')
        .shard_iterator
    end
  end

  def client
    @client ||= Aws::DynamoDBStreams::Client.new(Aws.config[:dynamodb])
  end

  def dynamodb
    @dynamodb ||= Aws::DynamoDB::Client.new
  end
end
