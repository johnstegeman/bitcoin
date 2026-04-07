#!/usr/bin/env fish

# Usage: tail -f progress.json.log | fish progress_watch.fish

while read -l line
    test -z "$line"; and continue

    # Parse everything in one jq call, tab-separated
    set data (echo $line | jq -r '
        def pct(a;b): if b > 0 then (a/b*100 | tostring | .[0:5]) + "%" else "?" end;
        def fmt(n): n | tostring | gsub("(?<=\\d)(?=(\\d{3})+$)"; ",");

        [
            (.nodeStats.created | tostring),
            (.estimatedTotalNumberOfNodes | tostring),
            (.relationshipStats.created | tostring),
            (.estimatedTotalNumberOfRelationships | tostring),
            (.relationshipImportDuration / 1000 | floor | tostring),
            (
                .nodePerLabelStats
                | to_entries
                | map(.key + "\t" + (.value.created | tostring))
                | join("\n")
            ),
            "---",
            (
                .relationshipPerTypeStats
                | to_entries
                | map(.key + "\t" + (.value.created | tostring))
                | join("\n")
            )
        ] | join("\n")
    ' 2>/dev/null)

    test -z "$data"; and continue

    clear

    # Parse lines
    set nodes_created  (echo $data | string split '\n')[1]
    set nodes_total    (echo $data | string split '\n')[2]
    set rels_created   (echo $data | string split '\n')[3]
    set rels_total     (echo $data | string split '\n')[4]
    set rel_dur_s      (echo $data | string split '\n')[5]

    # Compute percentages
    set node_pct (math --scale=1 "$nodes_created / $nodes_total * 100")
    if test "$rels_total" -gt 0
        set rel_pct (math --scale=1 "$rels_created / $rels_total * 100")
    else
        set rel_pct "0.0"
    end

    # Format rel duration
    set rel_h   (math --scale=0 "floor($rel_dur_s / 3600)")
    set rel_m   (math --scale=0 "floor(($rel_dur_s % 3600) / 60)")
    set rel_s2  (math --scale=0 "$rel_dur_s % 60")

    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    printf "  Neo4j Import Progress   %s\n" (date '+%H:%M:%S')
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    printf "\n  %-14s %18s / %-18s  %s%%\n" \
        "Nodes" \
        (printf "%'d" $nodes_created) \
        (printf "%'d" $nodes_total) \
        $node_pct

    printf "  %-14s %18s / %-18s  %s%%\n" \
        "Relationships" \
        (printf "%'d" $rels_created) \
        (printf "%'d" $rels_total) \
        $rel_pct

    printf "  %-14s %dh %dm %ds\n\n" "Rel duration" $rel_h $rel_m $rel_s2

    # Node label stats
    echo "  Node labels:"
    printf "  %-18s %s\n" "─────────────────" "──────────────────"
    for entry in (echo $data | string split '\n' | string match -v '---' | tail -n +6)
        set parts (string split '\t' $entry)
        test (count $parts) -lt 2; and continue
        test -z $parts[1]; and break
        printf "  %-18s %s\n" $parts[1] (printf "%'d" $parts[2])
    end

    echo ""

    # Relationship type stats — find lines after ---
    set after_sep 0
    echo "  Relationship types:"
    printf "  %-22s %s\n" "──────────────────────" "──────────────────"
    for entry in (echo $data | string split '\n')
        if test "$entry" = "---"
            set after_sep 1
            continue
        end
        test $after_sep -eq 0; and continue
        set parts (string split '\t' $entry)
        test (count $parts) -lt 2; and continue
        printf "  %-22s %s\n" $parts[1] (printf "%'d" $parts[2])
    end

    echo ""
end
