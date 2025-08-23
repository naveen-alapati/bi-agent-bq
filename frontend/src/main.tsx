import React from 'react'
import ReactDOM from 'react-dom/client'
import { createBrowserRouter, RouterProvider } from 'react-router-dom'
import App from './pages/App'
import Home from './pages/Home'
import KPIDraft from './pages/KPIDraft'

const router = createBrowserRouter([
  { path: '/', element: <Home /> },
  { path: '/editor', element: <App /> },
  { path: '/editor/:id', element: <App /> },
  { path: '/kpi-draft', element: <KPIDraft /> },
])

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <RouterProvider router={router} />
  </React.StrictMode>
)