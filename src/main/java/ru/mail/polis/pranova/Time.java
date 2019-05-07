package ru.mail.polis.pranova;

public final class Time {
    private static int count;
    private static long lastTime;

    private Time() {
    }

    /**
     * Method for return current time in nano seconds.
     *
     * @return time
     */
    public static long currentTime() {
        synchronized (Time.class) {
            final var time = System.currentTimeMillis();
            if (lastTime != time) {
                lastTime = time;
                count = 0;
            }
            return lastTime * 1_000_000 + count++;
        }
    }
}
