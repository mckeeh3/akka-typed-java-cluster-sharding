package cluster;

import akka.cluster.sharding.ShardRegion;

class EntityMessage {

  interface Command extends CborSerializable {}

  static class EntityCommand implements CborSerializable {
    final Entity entity;

    EntityCommand(Entity entity) {
      this.entity = entity;
    }

    @Override
    public String toString() {
      return String.format("%s[%s]", getClass().getSimpleName(), entity);
    }
  }

  static class EntityCommandAck implements CborSerializable {
    final String action;
    final Entity entity;

    EntityCommandAck(String action, Entity entity) {
      this.action = action;
      this.entity = entity;
    }

    @Override
    public String toString() {
      return String.format("%s[%s, %s]", getClass().getSimpleName(), action, entity);
    }
  }

  static class EntityQuery implements CborSerializable {
    final Entity.Id id;

    EntityQuery(Entity.Id id) {
      this.id = id;
    }

    @Override
    public String toString() {
      return String.format("%s[%s]", getClass().getSimpleName(), id);
    }
  }

  static class EntityQueryAck implements CborSerializable {
    final Entity entity;

    EntityQueryAck(Entity entity) {
      this.entity = entity;
    }

    @Override
    public String toString() {
      return String.format("%s[%s]", getClass().getSimpleName(), entity);
    }
  }

  static class EntityQueryAckNotFound implements CborSerializable {
    final Entity.Id id;

    EntityQueryAckNotFound(Entity.Id id) {
      this.id = id;
    }

    @Override
    public String toString() {
      return String.format("%s[%s]", getClass().getSimpleName(), id);
    }
  }

  static class Action implements CborSerializable {
    final String member;
    final String shardId;
    final String entityId;
    final String action;
    final boolean forward;

    Action(
      String member,
      String shardId,
      String entityId,
      String action,
      boolean forward
    ) {
      this.member = member;
      this.shardId = shardId;
      this.entityId = entityId;
      this.action = action;
      this.forward = forward;
    }

    Action asNoForward() {
      return new Action(member, shardId, entityId, action, false);
    }

    @Override
    public String toString() {
      return String.format(
        "%s[%s, %s, %s, %s, %b]",
        getClass().getSimpleName(),
        member,
        shardId,
        entityId,
        action,
        forward
      );
    }
  }

  static ShardRegion.MessageExtractor messageExtractor() {
    return new ShardRegion.MessageExtractor() {

      @Override
      public String shardId(Object message) {
        return extractShardIdFromCommands(message);
      }

      @Override
      public String entityId(Object message) {
        return extractEntityIdFromCommands(message);
      }

      @Override
      public Object entityMessage(Object message) {
        return message;
      }
    };
  }

  static String extractShardIdFromCommands(Object message) {
    int numberOfShards = 15;

    if (message instanceof Command) {
      return ((EntityCommand) message).entity.id.id.hashCode() % numberOfShards + "";
    } else if (message instanceof EntityQuery) {
      return ((EntityQuery) message).id.id.hashCode() % numberOfShards + "";
    } else {
      return null;
    }
  }

  static String extractEntityIdFromCommands(Object message) {
    if (message instanceof Command) {
      return ((EntityCommand) message).entity.id.id;
    } else if (message instanceof EntityQuery) {
      return ((EntityQuery) message).id.id;
    } else {
      return null;
    }
  }
}
