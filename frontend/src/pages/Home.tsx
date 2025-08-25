import React, { useEffect, useMemo, useRef, useState } from 'react'
import { api } from '../services/api'
import GridLayout, { Layout } from 'react-grid-layout'
import { ChartRenderer } from '../ui/ChartRenderer'
import '../styles.css'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter'
import { oneDark } from 'react-syntax-highlighter/dist/esm/styles/prism'
import html2canvas from 'html2canvas'
import jsPDF from 'jspdf'
import { createRoot } from 'react-dom/client'
import { computeKpiLineage, Lineage } from '../utils/lineage'

  const exportDropdownRef = useRef<HTMLDivElement>(null)
  const [lineageOpen, setLineageOpen] = useState(false)
  const [lineageKpi, setLineageKpi] = useState<any>(null)
  const [lineageData, setLineageData] = useState<Lineage | null>(null)

  function openLineage(k: any) {
    try {
      const lin = computeKpiLineage(k.sql, k)
      setLineageKpi(k)
      setLineageData(lin)
      setLineageOpen(true)
    } catch (e) {
      setLineageKpi(k)
      setLineageData({ sources: [], joins: [] } as any)
      setLineageOpen(true)
    }
  }
