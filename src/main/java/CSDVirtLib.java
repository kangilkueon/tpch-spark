package csdvirt;


public class CSDVirtLib {
    private static class Holder {
        static CSDVirtLib instance = new CSDVirtLib();
    }

    public static CSDVirtLib get() {
        return Holder.instance;
    }

    private CSDVirtLib() {
        System.loadLibrary("CSDVirtJ");
    }
}
