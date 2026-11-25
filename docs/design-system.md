# Design system

Phase 1 has no user interface. These tokens are recorded so a later visualisation can use the same identity as the rest of the consultancy demos.

The source is `abc-prediction-platform`, files `frontend/src/styles/tokens.css` and `frontend/src/styles/base.css`. Do not change that project from this repository. Do not introduce a second palette.

## Colour

| Token | Hex | Use |
| --- | --- | --- |
| `ink` | `#1c1b19` | Primary text |
| `ink-muted` | `#5c5a55` | Secondary text and labels |
| `paper` | `#f4f2ed` | Surfaces |
| `paper-elevated` | `#faf9f6` | Raised surfaces |
| `page` | `#e8e6e1` | Page background behind the paper |
| `rule` | `#d4d0c8` | Hairline borders |
| `accent` | `#2f4a3c` | Primary actions and key figures |
| `accent-soft` | `#e6ebe7` | Quiet accent fills |
| `on-accent` | `#f7f5f0` | Text on the accent colour |
| Accent hover | `#243a30` | Hover state for accent fills |

## Type

- Headings: Newsreader, then Georgia, then `"Times New Roman"`, serif. Weight 500. Slight negative tracking.
- Body and UI: Source Sans 3, then Segoe UI, sans-serif.
- Section labels: Source Sans 3, 600 weight, uppercase, wide tracking.

## Shape

- Corner radius: 2px.
- Borders: 1px `rule`, or 1px `accent` on a primary action.

## Later visualisation

Phase 1 does not add a theme file or a report. A later interface can map `ink` to text, `paper` and `page` to background, `accent` to the primary data colour, and `rule` to borders or gridlines. Use Newsreader for titles where it is available, and Source Sans 3 or Segoe UI for labels.
