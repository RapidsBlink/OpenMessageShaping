package io.openmessaging.demo;

/**
 * Created by cheyulin on 27/05/2017.
 */
public class YchePair<K, V> {
    public final K key;
    public final V val;

    public YchePair(K key, V val) {
        this.key = key;
        this.val = val;
    }
}
