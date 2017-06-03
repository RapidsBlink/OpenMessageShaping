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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        YchePair<?, ?> ychePair = (YchePair<?, ?>) o;

        if (key != null ? !key.equals(ychePair.key) : ychePair.key != null) return false;
        return val != null ? val.equals(ychePair.val) : ychePair.val == null;
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (val != null ? val.hashCode() : 0);
        return result;
    }
}
