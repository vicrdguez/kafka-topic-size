package com.github.vicrdguez;

import java.util.regex.Pattern;

// simply printing out these codes is all it takes to make
// use of ANSI Escape codes. You can add colours as well as
// other fancy animation too.
// https://gist.github.com/fnky/458719343aabd01cfb17a3a4f7296797
public class Graphics {
    // private static Pattern cursorReqPattern = Pattern.compile("ESC\\[(\\d+);(\\d+)R");

    private static final String CLEAR_SCREEN = "\u001b[2J";
    private static final String CLEAR_FROM_CURSOR = "\u001b[0J";
    private static final String ERASE_LINE = "\u001b[2K";
    private static final String LINE_UP = "\u001b[%dA";
    private static final String LINE_DOWN = "\u001b[%dB";
    private static final String GOTO_COORD = "\u001b[%d;%dH";
    private static final String SHOW_CURSOR = "\u001b[?25h";
    private static final String HIDE_CURSOR = "\u001b[?25l";
    private static final String SAVE_CURSOR = "\u001b 7";
    private static final String RESTORE_CURSOR = "\u001b 8";
    private static final String BOLD = "\033[1;37m%s\033[0m"; 

    public static void cls() {
        System.out.println(CLEAR_SCREEN);
    }

    public static void fromCursor(){
        System.out.print(CLEAR_FROM_CURSOR);
    }

    public static void gotoXY(int x, int y) {
        System.out.print(String.format(GOTO_COORD, x, y));
    }

    public static void showCursor() {
        System.out.print(SHOW_CURSOR);
    }

    public static void hideCursor() {
        System.out.print(HIDE_CURSOR);
    }

    public static void saveCursor(){
        System.out.print(SAVE_CURSOR);
    }
    public static void restoreCursor(){
        System.out.print(RESTORE_CURSOR);
    }
    public static void eraseLine(){
        System.out.print(ERASE_LINE);
    }

    public static void lineUp(int x){
        System.out.print(String.format(LINE_UP, x));
    }

    public static void lineDown(int x){
        System.out.print(String.format(LINE_DOWN, x));
    }

    public static String bold(String s){
        return String.format(BOLD, s);
    }
}
