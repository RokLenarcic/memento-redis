package memento.redis.poll;

import memento.caffeine.SpecialPromise;

public class Load {
    private final SpecialPromise promise;
    private volatile Object loadMarker;

    public Load(Object loadMarker) {
        this.promise = new SpecialPromise();
        this.loadMarker = loadMarker;
    }

    public SpecialPromise getPromise() {
        return promise;
    }

    public Object getLoadMarker() {
        return loadMarker;
    }

    public void setLoadMarker(Object loadMarker) {
        this.loadMarker = loadMarker;
    }

    public void ourLoad() {
        promise.init();
    }

    public void foreignLoad() {
        loadMarker = null;
    }

}