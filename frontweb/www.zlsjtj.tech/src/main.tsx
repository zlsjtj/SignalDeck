import 'antd/dist/reset.css';
import React from 'react';
import ReactDOM from 'react-dom/client';

import { AppProviders } from '@/AppProviders';
import '@/styles/global.css';

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <AppProviders />
  </React.StrictMode>,
);
