require "etc"
require "aws-sdk"

module FakeSNS
  class Database

    attr_reader :database_filename
    attr_reader :settings

    def initialize(settings)
      @database_filename = settings.database || File.join(Dir.home, ".fake_sns.yml")
      @settings = settings
    end

    def perform(action, params)
      action_instance = action_provider(action).new(self, params)
      action_instance.call
      Response.new(action_instance)
    end

    def topics
      @topics ||= TopicCollection.new(store)
    end

    def subscriptions
      @subscriptions ||= SubscriptionCollection.new(store)
    end

    def messages
      @messages ||= MessageCollection.new(store)
    end

    def reset
      topics.reset
      subscriptions.reset
      messages.reset
    end

    def transaction
      store.transaction do
        yield
      end
    end

    def replace(data)
      store.replace(data)
    end

    def to_yaml
      store.to_yaml
    end

    def each_deliverable_message
      topics.each do |topic|
        subscriptions.each do |subscription|
          if subscription.topic_arn != topic.arn then
            next
          end
          messages.each do |message|
            if message.topic_arn == subscription.topic_arn
              yield subscription, message
            end
          end
        end
      end
    end

    def deliver_message(message_id)
      each_deliverable_message do |subscription, message|
        if message.id == message_id
          DeliverMessage.call(subscription: subscription, message: message, config: @settings)
        end
      end
    end

    private

    def store
      @store ||= Storage.for(database_filename)
    end

    def action_provider(action)
      Actions.const_get(action)
    rescue NameError
      raise InvalidAction, "not implemented: #{action}"
    end

  end
end
