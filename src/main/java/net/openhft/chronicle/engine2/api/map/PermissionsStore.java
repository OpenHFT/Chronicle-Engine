package net.openhft.chronicle.engine2.api.map;

import net.openhft.chronicle.engine2.api.Permissions;

/**
 * Tag interface for AuthenticatingStores
 */
public interface PermissionsStore<MV> extends SubscriptionKeyValueStore<String, MV, Permissions>{
}
