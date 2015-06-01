package net.openhft.chronicle.engine;

import net.openhft.chronicle.map.FPMEvent;
import net.openhft.chronicle.map.FilePerKeyMap;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Created by daniel on 01/05/15.
 */
public class FilePerKeyMapSubscription implements Subscription{

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(FilePerKeyMapSubscription.class);
    private FilePerKeyMap filePerKeyMap;
    private boolean subscribeAll = false;
    private Set<String> subscribedKeys = new HashSet<>();

    public FilePerKeyMapSubscription(FilePerKeyMap filePerKeyMap) {
        this.filePerKeyMap = filePerKeyMap;
    }

    @Override
    public void subscribeAll() {
        subscribeAll = true;
    }

    @Override
    public void subscribe(Object[] keys) {
        Arrays.stream(keys).forEach(k->subscribedKeys.add((String)k));
    }

    @Override
    public void unsubscribeAll() {
        subscribeAll = false;
        unsubscribe(subscribedKeys.toArray(new Object[0]));
    }

    @Override
    public void unsubscribe(Object[] keys) {
        Arrays.stream(keys).forEach(k->subscribedKeys.remove((String) k));
    }

    @Override
    public void setCallback(Object callback) {
        MapEventListener mel = (MapEventListener)callback;
        Consumer<FPMEvent> fpmEventConsumer = (FPMEvent e) -> {
            LOG.error("", e);
            if(subscribeAll || subscribedKeys.contains(e.getKey())) {
                mel.update(e.getKey(), e.getLastValue(), e.getValue());
            }
        };
        filePerKeyMap.registerForEvents(fpmEventConsumer);
    }
}
