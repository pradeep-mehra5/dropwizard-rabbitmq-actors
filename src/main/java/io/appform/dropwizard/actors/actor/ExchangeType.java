package io.appform.dropwizard.actors.actor;

public enum ExchangeType {
    DIRECT {
        @Override
        public String amqpType() {
            return "direct";
        }

        @Override
        public <T> T handleConfig(ExchangeTypeVisitor<T> visitor) {
            return visitor.visitDirect();
        }
    },
    TOPIC {
        @Override
        public String amqpType() {
            return "topic";
        }

        @Override
        public <T> T handleConfig(ExchangeTypeVisitor<T> visitor) {
            return visitor.visitTopic();
        }
    },
    FANOUT {
        @Override
        public String amqpType() {
            return "fanout";
        }

        @Override
        public <T> T handleConfig(ExchangeTypeVisitor<T> visitor) {
            return visitor.visitFanout();
        }
    };

    public abstract String amqpType();

    public abstract <T> T handleConfig(ExchangeTypeVisitor<T> visitor);
}
