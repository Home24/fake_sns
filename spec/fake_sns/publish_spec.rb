RSpec.describe "Publishing", :sqs do

  let(:existing_topic) { sns.create_topic(name: "my-topic").topic_arn }

  it "remembers published messages" do
    message_id = sns.publish(topic_arn: existing_topic, message: "hallo").message_id
    messages = $fake_sns.data.fetch("messages")
    expect(messages.size).to eq 1
    message = messages.first
    expect(message.fetch(:id)).to eq message_id
  end

  it "needs an existing topic" do
    non_existing = "arn:aws:sns:us-east-1:5068edfd0f7ee3ea9ccc1e73cbb17569:not-exist"
    expect {
      sns.publish(topic_arn: non_existing, message: "hallo")
    }.to raise_error Aws::SNS::Errors::InvalidParameterValue
  end

  it "doesn't allow messages that are too big" do
    expect {
      sns.publish(topic_arn: existing_topic, message: "A" * 262145)
    }.to raise_error Aws::SNS::Errors::InvalidParameterValue
  end

  it "publishes messages to sqs" do
    queue_url = sqs.create_queue(queue_name: "my-queue").queue_url
    queue_arn = sqs.get_queue_attributes(queue_url: queue_url, attribute_names: ["QueueArn"]).attributes.fetch("QueueArn")
    topic_arn = sns.create_topic(name: "my-topic").topic_arn

    _subscription_arn = sns.subscribe(topic_arn: topic_arn, protocol: "sqs", endpoint: queue_arn).subscription_arn

    sns.publish(topic_arn: topic_arn, message: { sqs: "hallo" }.to_json)

    attributes = sqs.get_queue_attributes(queue_url: queue_url, attribute_names: ["ApproximateNumberOfMessages"]).attributes
    expect(attributes.fetch("ApproximateNumberOfMessages")).to eq "1"

    received = sqs.receive_message(queue_url: queue_url)
    body = received.messages[0].body
    unserialized_body = JSON.parse(body)

    expect(unserialized_body["Message"]).to eq 'hallo'
  end

end
