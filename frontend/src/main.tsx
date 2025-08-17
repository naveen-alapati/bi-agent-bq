import React from 'react'
import ReactDOM from 'react-dom/client'
import { createBrowserRouter, RouterProvider } from 'react-router-dom'
import App from './pages/App'
import Home from './pages/Home'

const router = createBrowserRouter([
  { path: '/', element: <Home /> },
  { path: '/editor', element: <App /> },
  { path: '/editor/:id', element: <App /> },
])

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <RouterProvider router={router} />
  </React.StrictMode>
)