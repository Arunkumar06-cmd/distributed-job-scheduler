import React from 'react'
export default class ErrorBoundary extends React.Component {
  constructor(props){ super(props); this.state = { err: null } }
  static getDerivedStateFromError(err){ return { err } }
  render(){
    return this.state.err
      ? <div className="app"><div className="notice error">Something went wrong: {String(this.state.err)}<button onClick={() => this.setState({ err: null })}>Reload</button></div></div>
      : this.props.children
  }
}
