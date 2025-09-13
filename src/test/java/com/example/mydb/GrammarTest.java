package com.example.mydb;

import org.csu.mydb.compiler.Grammar;

import java.util.List;

/**
 * @author ljyljy
 * @date 2025/9/12
 * @description grammar test
 */
public class GrammarTest {
    // Debug: 打印产生式
    public static void main(String[] args) {
        Grammar g = new Grammar();
        System.out.println("=== 产生式 ===");
        for (Grammar.NonTerminal nt : g.nonTerminals) {
            List<Grammar.Production> ps = g.getProductions(nt);
            for (Grammar.Production p : ps) System.out.println(p);
        }
        g.computeFirstSets();
        g.computeFollowSets();
        g.printFirstSets();
        g.printFollowSets();
    }
}
